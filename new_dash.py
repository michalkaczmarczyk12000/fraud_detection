import threading
import json
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from collections import Counter, deque
from kafka import KafkaConsumer
import queue
import numpy as np
from matplotlib.gridspec import GridSpec


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
        self.transaction_count = 0
        self.anomaly_count = 0
        self.fraud_counts = Counter()
        self.message_queue = queue.Queue()
        self.last_two_transactions = deque(maxlen=2)

        self.kafka_thread_transactions = KafkaConsumerThread(
            "Transaction", self.message_queue
        )
        self.kafka_thread_frauds = KafkaConsumerThread("Anomaly", self.message_queue)

        self.kafka_thread_transactions.start()
        self.kafka_thread_frauds.start()

        self.fig = plt.figure(figsize=(12, 10))
        gs = GridSpec(3, 1, height_ratios=[2, 2, 1])

        self.ax1 = self.fig.add_subplot(gs[0])
        self.ax2 = self.fig.add_subplot(gs[1])
        self.ax3 = self.fig.add_subplot(gs[2])

        self.colors = plt.cm.tab20(np.linspace(0, 1, 40))

        self.ani = animation.FuncAnimation(self.fig, self.update_plots, interval=1000)
        plt.tight_layout(pad=2.0)
        plt.show()

    def process_messages(self):
        while not self.message_queue.empty():
            topic, message = self.message_queue.get()
            print(f"Processing message from topic {topic}: {message}")
            if topic == "Transaction":
                self.transaction_count += 1
                self.last_two_transactions.append(message)
            elif topic == "Anomaly":
                self.anomaly_count += 1
                self.fraud_counts[message["anomaly"]] += 1
                self.last_two_transactions.append(message)

    def update_table(self):
        table_data = [
            [txn["card_id"], txn["user_id"], txn["value"], txn["limit"], txn["timestamp"], txn.get("anomaly", "N/A")]
            for txn in self.last_two_transactions
        ]

        self.ax3.clear()
        self.ax3.axis("off")

        table = self.ax3.table(
            cellText=table_data,
            colLabels=["Card ID", "User ID", "Value", "Limit", "Timestamp", "Anomaly"],
            loc="center",
            cellLoc="center",
        )

        for i, row in enumerate(table_data):
            anomaly = row[5]
            cell_color = "red" if anomaly != "N/A" else "green"
            for j in range(len(row)):
                table[(i+1, j)].set_facecolor(cell_color)

    def update_plots(self, _):
        self.process_messages()

        self.ax1.clear()
        self.ax2.clear()

        self.plot_counts(self.ax1, self.transaction_count, self.anomaly_count)
        self.plot_frauds(self.ax2)

        self.update_table()

    def plot_counts(self, ax, transaction_count, anomaly_count):
        counts = [transaction_count, anomaly_count]
        labels = ["Valid Transactions", "Anomalies"]

        ax.bar(labels, counts, color=["blue", "red"])
        ax.set_title("Transaction and Anomaly Counts")
        ax.set_ylabel("Count")

        for i, count in enumerate(counts):
            ax.text(i, count + 0.5, str(count), ha="center")

    def plot_frauds(self, ax):
        fraud_types = list(self.fraud_counts.keys())
        fraud_counts = list(self.fraud_counts.values())

        bar_colors = [self.colors[hash(t) % len(self.colors)] for t in fraud_types]

        ax.bar(fraud_types, fraud_counts, color=bar_colors)
        ax.set_title("Detected Fraud Types")
        ax.set_xlabel("Fraud Type")
        ax.set_ylabel("Count")

        for i, count in enumerate(fraud_counts):
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
