import logging
import sys
from typing import Iterable
import os

import redis
from datetime import datetime, timedelta
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


class DetectLocationChange(KeyedProcessFunction):

    def open(self, runtime_context):
        print(">open of DetectLocationChange")
        self.redis_host = os.getenv("REDIS_HOST", "localhost")
        self.redis_port = int(os.getenv("REDIS_PORT", 6379))
        self.redis_client = redis.StrictRedis(
            host=self.redis_host, port=self.redis_port, decode_responses=True
        )
        print(f"Connected to Redis at {self.redis_host}:{self.redis_port}")

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        try:
            card_id = value["card_id"]
            user_id = value["user_id"]
            latitude = value["latitude"]
            longitude = value["longitude"]
            timestamp = datetime.strptime(value["timestamp"], "%Y-%m-%d %H:%M:%S")
            print("=====[START] Location change anomaly detection started=====")
            print(f"Processing transaction for card_id={card_id}, user_id={user_id}")

            last_location = self.redis_client.get(user_id)

            if last_location:
                last_lat, last_lon, last_timestamp_str = last_location.split(",")
                last_lat, last_lon = float(last_lat), float(last_lon)
                last_timestamp = datetime.strptime(
                    last_timestamp_str, "%Y-%m-%d %H:%M:%S"
                )

                time_diff = (
                                    timestamp - last_timestamp
                            ).total_seconds() / 3600  # in hours
                distance = haversine(last_lat, last_lon, latitude, longitude)
                speed = distance / time_diff
                print(f"Calculated speed for user_id={user_id} is {speed} km/h")

                if (
                        speed > 90  # 900
                ):  # Speed greater than 900 km/h (faster than a passenger plane)
                    print("Anomaly detected")
                    value["anomaly"] = "High speed detected"
                    yield value
            print(f"Saving last location of user {user_id}")
            self.redis_client.set(
                user_id,
                f"{latitude},{longitude},{timestamp.strftime('%Y-%m-%d %H:%M:%S')}",
            )
            print("=====[END] Location change anomaly detection =====")
        except Exception as e:
            print(f"Error processing element. ", e)


class DetectTransactionValueChange(KeyedProcessFunction):

    def open(self, runtime_context):
        print(">open of DetectTransactionValueChange")
        self.redis_host = os.getenv("REDIS_HOST", "localhost")
        self.redis_port = int(os.getenv("REDIS_PORT", 6379))
        self.redis_client = redis.StrictRedis(
            host=self.redis_host, port=self.redis_port, decode_responses=True
        )
        logging.info(f"Connected to Redis at {self.redis_host}:{self.redis_port}")

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        user_id = value["user_id"]
        transaction_value = float(value["value"])
        print("=====[START] Transaction value chage anomaly detection started=====")
        print(f"Processing transaction for user_id={user_id}")

        avg_key = f"{user_id}_avg_value"
        avg_transaction_value = self.redis_client.get(avg_key)
        if avg_transaction_value is None:
            avg_transaction_value = 0.0
        else:
            avg_transaction_value = float(avg_transaction_value)

        new_avg_value = (avg_transaction_value + transaction_value) / 2
        print(f"New average transaction value for {user_id}: {new_avg_value}")

        if transaction_value > 2 * avg_transaction_value:
            print("Anomaly detected: Significant increase in transaction value")
            value["anomaly"] = "Significant increase in transaction value"
            yield value

        self.redis_client.set(avg_key, new_avg_value)
        print("=====[END] Transaction value chage anomaly detection started=====")


class DetectFrequentTransactions(ProcessWindowFunction[Row, Row, int, TimeWindow]):
    def process(
            self, key: int, context: ProcessWindowFunction.Context, elements: Iterable[Row]
    ) -> Iterable[Row]:
        transactions = list(elements)
        print("=====[START] Frequent transactions anomaly detection =====")
        print(f"Processing transactions {transactions}")
        if len(transactions) > 3:
            print("Anomaly detected")
            for transaction in transactions:
                transaction["anomaly"] = "High frequency of transactions"
                yield transaction
        print("=====[END] Frequent transactions anomaly detection =====")


class DetectLimitBreaches(ProcessWindowFunction):

    # def open(self, runtime_context: RuntimeContext):
    #     print(">open of DetectLimitBreaches")

    def process(self, key: int, context: ProcessWindowFunction.Context, elements: Iterable[dict]) -> Iterable[dict]:
        transactions = list(elements)
        print("=====[START] Card limit exceeded anomaly detection =====")
        print(f"Processing transactions: {transactions}")

        # Pobierz limit dla karty
        limit = float(transactions[0]["limit"])
        print(f"limit = {limit}")
        over_limit_transactions = [trans for trans in transactions if float(trans["value"]) > limit]

        print(f"Transactions over the limit: {over_limit_transactions}")

        if len(over_limit_transactions) >= 3:
            print("Anomaly detected")
            for transaction in over_limit_transactions:
                transaction["anomaly"] = "Limit breach detected"
                yield transaction

        print("=====[END] Card limit exceeded anomaly detection =====")


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

    env.add_jars("file:///app/flink-sql-connector-kafka-3.1.0-1.18.jar")

    input_type_info = Types.ROW_NAMED(
        [
            "card_id",
            "user_id",
            "latitude",
            "longitude",
            "value",
            "limit",
            "timestamp",
            "anomaly",
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

    deserialization_schema = (
        JsonRowDeserializationSchema.builder()
        .type_info(type_info=input_type_info)
        .build()
    )

    kafka_source = FlinkKafkaConsumer(
        topics="Transaction",
        deserialization_schema=deserialization_schema,
        properties={"bootstrap.servers": "kafka:9092"},
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

    anomalies_location = ds.key_by(lambda row: row["card_id"]).process(
        DetectLocationChange(), output_type=output_type_info
    )

    anomalies_frequent = (
        ds.key_by(lambda row: row["user_id"])
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .process(DetectFrequentTransactions(), output_type=output_type_info)
    )

    anomalies_limit = (
        ds.key_by(lambda row: row["card_id"])
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .process(DetectLimitBreaches(), output_type=output_type_info)
    )

    anomalies_value_change = ds.key_by(lambda row: row["user_id"]).process(
        DetectTransactionValueChange(), output_type=output_type_info
    )

    serialization_schema = (
        JsonRowSerializationSchema.builder()
        .with_type_info(type_info=output_type_info)
        .build()
    )

    kafka_sink = FlinkKafkaProducer(
        topic="Anomaly",
        serialization_schema=serialization_schema,
        producer_config={"bootstrap.servers": "kafka:9092"},
    )

    anomalies_location.add_sink(kafka_sink)
    anomalies_frequent.add_sink(kafka_sink)
    anomalies_limit.add_sink(kafka_sink)
    anomalies_value_change.add_sink(kafka_sink)

    env.execute("anomaly_detection")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    main()
