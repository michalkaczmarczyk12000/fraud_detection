import threading
import json
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from collections import Counter
from kafka import KafkaConsumer
import queue

import numpy as np


class KafkaConsumerThread(threading.Thread):
    def __init__(self, topic, message_queue):
        super().__init__()
        self.topic = topic
        self.message_queue = message_queue
        self.running = True
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers="localhost:9093",
            auto_offset_reset="earliest",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )

    def run(self):
        while self.running:
            for message in self.consumer:
                if not self.running:
                    break
                self.message_queue.put((self.topic, message.value))
        self.consumer.close()

    def stop(self):
        self.running = False


class TransactionMonitor:
    def __init__(self):
        self.transaction_counts = Counter()
        self.fraud_counts = Counter()

        self.message_queue = queue.Queue()

        self.kafka_thread_transactions = KafkaConsumerThread(
            "Transaction", self.message_queue
        )
        self.kafka_thread_frauds = KafkaConsumerThread("Anomaly", self.message_queue)

        self.kafka_thread_transactions.start()
        self.kafka_thread_frauds.start()

        self.fig, (self.ax1, self.ax2, self.table) = plt.subplots(1, 3, figsize=(12, 8))
        self.colors = plt.cm.tab20(np.linspace(0, 1, 40))

        self.ani = animation.FuncAnimation(self.fig, self.update_plots, interval=1000)
        plt.tight_layout(pad=4.0)
        plt.suptitle("Frauds Detection Analysis", fontsize=16)
        plt.show()

    def process_messages(self):
        while not self.message_queue.empty():
            topic, message = self.message_queue.get()
            print(message)
            print(f"Processing message from topic {topic}: {message}")
            if topic == "Transaction":
                self.transaction_counts[message["user_id"]] += 1
            elif topic == "Anomaly":
                self.fraud_counts[message["anomaly"]] += 1

    def update_table(self):
        table_data = []

        for transaction_type in self.transaction_counts:
            generated = self.transaction_counts[transaction_type]
            detected = self.fraud_counts[transaction_type]
            is_valid = self.validate_transaction(transaction_type, generated, detected)
            table_data.append([transaction_type, generated, detected, is_valid])

        if not table_data:
            return

        self.table.clear()
        self.table.axis("off")
        self.table.table(
            cellText=table_data,
            colLabels=["Type", "Generated", "Detected", "IsValid"],
            loc="center",
            cellLoc="center",
        )

        table = self.table.table(
            cellText=table_data,
            colLabels=["Type", "Generated", "Detected", "IsValid"],
            loc="center",
            cellLoc="center",
        )
        for i, row in enumerate(table_data):
            is_valid = row[3]
            cell_color = "green" if is_valid else "red"
            table.get_celld()[(i + 1, 3)].set_facecolor(cell_color)

    def validate_transaction(self, transaction_type, generated, detected):
        if transaction_type == "LT":
            return generated == 2 * detected
        elif transaction_type == "VT":
            return generated == 2 * detected
        elif transaction_type == "SL":
            return generated == detected
        return True

    def update_plots(self, _):
        self.process_messages()

        self.ax1.clear()
        self.ax2.clear()

        self.plot_transactions(
            self.ax1, self.transaction_counts, "Generated Transactions"
        )
        self.plot_transactions(
            self.ax2, self.fraud_counts, "Anomaly Transactions"
        )

        self.update_table()

    def plot_transactions(self, ax, counts, title):
        transaction_types = list(counts.keys())
        transaction_counts = list(counts.values())

        bar_colors = [
            self.colors[hash(t) % len(self.colors)] for t in transaction_types
        ]

        ax.bar(transaction_types, transaction_counts, color=bar_colors)
        ax.set_title(title)
        ax.set_xlabel("Transaction Type")
        ax.set_ylabel("Count")

        for i, count in enumerate(transaction_counts):
            ax.text(i, count + 0.5, str(count), ha="center")

    def stop(self):
        self.kafka_thread_transactions.stop()
        self.kafka_thread_frauds.stop()


if __name__ == "__main__":
    monitor = TransactionMonitor()
    try:
        plt.show()
    except KeyboardInterrupt:
        monitor.stop()
        print("Stopped monitoring.")
