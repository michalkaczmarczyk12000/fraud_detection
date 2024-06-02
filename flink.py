import logging
import sys
from datetime import datetime, timedelta
from typing import Iterable

import redis
from pyflink.common import Types, Row
from pyflink.common.time import Time, Duration
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.formats.json import (
    JsonRowDeserializationSchema,
    JsonRowSerializationSchema,
)
from pyflink.datastream.functions import (
    KeyedProcessFunction,
    ProcessWindowFunction,
    RuntimeContext,
)
from pyflink.datastream.state import MapStateDescriptor
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.fn_execution.coder_impl_fast import TimeWindow

from util import haversine


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

    env.add_jars(
        "file:///home/bbaran/Desktop/psd2/flink-1.17.2/psd/flink-sql-connector-kafka-3.1.0-1.18.jar"
    )

    input_type_info = Types.ROW_NAMED(
        ["card_id", "user_id", "latitude", "longitude", "value", "limit", "timestamp"],
        [
            Types.INT(),
            Types.INT(),
            Types.DOUBLE(),
            Types.DOUBLE(),
            Types.INT(),
            Types.STRING(),
        ],
    )

    deserialization_schema = (
        JsonRowDeserializationSchema.builder()
        .type_info(type_info=input_type_info)
        .build()
    )

    kafka_source = FlinkKafkaConsumer(
        topics="Transaction",
        deserialization_schema=deserialization_schema,
        properties={"bootstrap.servers": "localhost:9092"},
    )

    ds = env.add_source(kafka_source)

    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_minutes(1)
    )

    ds = ds.assign_timestamps_and_watermarks(watermark_strategy)

    output_type_info = Types.ROW_NAMED(
        [
            "card_id",
            "user_id",
            "latitude",
            "longitude",
            "value",
            "limit",
            "anomaly",
            "timestamp",
        ],
        [
            Types.INT(),
            Types.INT(),
            Types.DOUBLE(),
            Types.DOUBLE(),
            Types.DOUBLE(),
            Types.DOUBLE(),
            Types.STRING(),
            Types.STRING(),
        ],
    )

    ds.print()

    anomalies = ds.key_by(lambda row: row["card_id"]).process(
        DetectLocationChange(), output_type=output_type_info
    )

    anomalies = (
        anomalies.key_by(lambda row: row["user_id"])
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .process(DetectFrequentTransactions(), output_type=output_type_info)
    )

    anomalies = anomalies.key_by(lambda row: row["card_id"]).process(
        DetectLimitBreaches(), output_type=output_type_info
    )

    serialization_schema = (
        JsonRowSerializationSchema.builder()
        .with_type_info(type_info=output_type_info)
        .build()
    )

    kafka_sink = FlinkKafkaProducer(
        topic="Anomaly",
        serialization_schema=serialization_schema,
        producer_config={"bootstrap.servers": "localhost:9092"},
    )

    anomalies.add_sink(kafka_sink)

    env.execute("anomaly_detection")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    main()


class DetectLocationChange(KeyedProcessFunction):

    def __init__(self, redis_host="localhost", redis_port=6379):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_client = None

    def open(self, runtime_context: RuntimeContext):
        self.redis_client = redis.StrictRedis(
            host=self.redis_host, port=self.redis_port, decode_responses=True
        )

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        card_id = value["card_id"]
        user_id = value["user_id"]
        latitude = value["latitude"]
        longitude = value["longitude"]
        timestamp = datetime.strptime(value["timestamp"], "%Y-%m-%d %H:%M:%S")

        last_location = self.redis_client.get(user_id)
        if last_location:
            last_lat, last_lon, last_timestamp_str = last_location.split(",")
            last_lat, last_lon = float(last_lat), float(last_lon)
            last_timestamp = datetime.strptime(last_timestamp_str, "%Y-%m-%d %H:%M:%S")

            time_diff = (timestamp - last_timestamp).total_seconds() / 3600  # in hours
            distance = haversine(last_lat, last_lon, latitude, longitude)
            speed = distance / time_diff

            if (
                speed > 900
            ):  # Speed greater than 900 km/h (faster than a passenger plane)
                value["anomaly"] = "High speed detected"
                yield value

        self.redis_client.set(user_id, f"{latitude},{longitude},{value['timestamp']}")


class DetectFrequentTransactions(ProcessWindowFunction[Row, Row, int, TimeWindow]):
    def process(
        self, key: int, context: ProcessWindowFunction.Context, elements: Iterable[Row]
    ) -> Iterable[Row]:
        transactions = list(elements)
        if len(transactions) > 3:
            for transaction in transactions:
                transaction["anomaly"] = "High frequency of transactions"
                yield transaction


class DetectLimitBreaches(KeyedProcessFunction):

    def open(self, runtime_context: RuntimeContext):
        self.transactions_state = runtime_context.get_map_state(
            MapStateDescriptor(
                "transactions_state", Types.LONG(), Types.PICKLED_BYTE_ARRAY()
            )
        )

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        current_time = datetime.strptime(value["timestamp"], "%Y-%m-%d %H:%M:%S")
        card_id = value["card_id"]
        limit = float(value["limit"])
        value_amount = float(value["value"])

        self.transactions_state.put(ctx.timestamp(), value)
        ctx.timer_service().register_event_time_timer(
            ctx.timestamp() + 60000
        )  # Set a timer for 1 minute later

        # Check for limit breaches
        over_limit_transactions = [
            trans
            for trans in self.transactions_state.values()
            if trans["value"] > limit
            and trans["timestamp"]
            > (current_time - timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
        ]

        if len(over_limit_transactions) >= 3:
            for trans in over_limit_transactions:
                trans["anomaly"] = "Limit breach detected"
                yield trans

    def on_timer(self, timestamp, ctx: "KeyedProcessFunction.OnTimerContext"):
        # Clear old state
        for ts in list(self.transactions_state.keys()):
            if ts <= timestamp:
                self.transactions_state.remove(ts)
