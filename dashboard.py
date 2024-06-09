import threading
import json
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Output, Input
import plotly.graph_objects as go
from kafka import KafkaConsumer
import queue
from collections import Counter

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
        print(1)
        while self.running:
            for message in self.consumer:
                if not self.running:
                    print(1)
                    break
                self.message_queue.put((self.topic, message.value))
                print(f"Received message: {message.value}")  # Add this line for debugging
        self.consumer.close()

    def stop(self):
        self.running = False

class TransactionMonitor:
    def __init__(self):
        self.transaction_counts = Counter()
        self.fraud_counts = Counter()
        self.message_queue = queue.Queue()

        self.kafka_thread_transactions = KafkaConsumerThread("Transaction", self.message_queue)
        self.kafka_thread_frauds = KafkaConsumerThread("Anomaly", self.message_queue)

        self.kafka_thread_transactions.start()
        self.kafka_thread_frauds.start()

        self.app = dash.Dash(__name__)
        self.app.layout = html.Div(
            [
                dcc.Graph(id="transaction-bar-graph"),
                dcc.Graph(id="fraud-bar-graph"),
                dcc.Interval(id="interval-component", interval=1 * 1000, n_intervals=0),
            ]
        )

        self.app.callback(Output("transaction-bar-graph", "figure"), [Input("interval-component", "n_intervals")])(self.update_transactions)
        self.app.callback(Output("fraud-bar-graph", "figure"), [Input("interval-component", "n_intervals")])(self.update_frauds)

    def stop(self):
        self.kafka_thread_transactions.stop()
        self.kafka_thread_frauds.stop()

    def update_transactions(self, n):
        while not self.message_queue.empty():
            topic, message = self.message_queue.get()
            if topic == "Transaction":
                self.transaction_counts[message["user_id"]] += 1
        return go.Figure(data=[go.Bar(x=list(self.transaction_counts.keys()), y=list(self.transaction_counts.values()))])

    def update_frauds(self, n):
        while not self.message_queue.empty():
            topic, message = self.message_queue.get()
            if topic == "Anomaly":
                self.fraud_counts[message["user_id"]] += 1
        return go.Figure(data=[go.Bar(x=list(self.fraud_counts.keys()), y=list(self.fraud_counts.values()))])

if __name__ == "__main__":
    monitor = TransactionMonitor()
    monitor.app.run_server(debug=True, use_reloader=False)
    monitor.stop()
