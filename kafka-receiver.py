from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "Anomaly",
    bootstrap_servers=["kafka:9092"],
    api_version=(0, 10),
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

if __name__ == "__main__":
    for message in consumer:
        message = message.value
        print(f"Received anomaly | Details = {message}")
