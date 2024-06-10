import threading
import json
import plotly.graph_objects as go
import dash
from dash import dcc, html, Input, Output
from kafka import KafkaConsumer
import queue
from collections import Counter
import time


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

        self.app = dash.Dash(__name__)
        self.app.layout = html.Div(
            [
                html.H1("Frauds Detection Analysis"),
                dcc.Graph(id="transaction-bar-chart"),
                dcc.Graph(id="fraud-bar-chart"),
                html.Table(id="validation-table"),
                dcc.Interval(
                    id="interval-component",
                    interval=1*1000,  # in milliseconds
                    n_intervals=0
                ),
            ]
        )

        self.app.callback(
            Output("transaction-bar-chart", "figure"),
            Output("fraud-bar-chart", "figure"),
            Output("validation-table", "children"),
            Input("interval-component", "n_intervals")
        )(self.update_plots)

    def process_messages(self):
        while not self.message_queue.empty():
            topic, message = self.message_queue.get()
            if topic == "Transaction":
                self.transaction_counts[message["user_id"]] += 1
            elif topic == "Anomaly":
                self.fraud_counts[message["anomaly"]] += 1

    def update_plots(self, n):
        self.process_messages()

        transaction_fig = self.plot_transactions(self.transaction_counts, "Current Generated Transactions")
        fraud_fig = self.plot_transactions(self.fraud_counts, "Detected Fraud Transactions")
        validation_table = self.update_table()

        return transaction_fig, fraud_fig, validation_table

    def plot_transactions(self, counts, title):
        transaction_types = list(counts.keys())
        transaction_counts = list(counts.values())

        fig = go.Figure(
            data=[
                go.Bar(
                    x=transaction_types,
                    y=transaction_counts,
                    text=transaction_counts,
                    textposition="auto"
                )
            ]
        )

        fig.update_layout(
            title=title,
            xaxis_title="Transaction Type",
            yaxis_title="Count"
        )

        return fig

    def update_table(self):
        table_data = []

        for transaction_type in self.transaction_counts:
            generated = self.transaction_counts[transaction_type]
            detected = self.fraud_counts[transaction_type]
            is_valid = self.validate_transaction(transaction_type, generated, detected)
            table_data.append([transaction_type, generated, detected, is_valid])

        if not table_data:
            return []

        header = [html.Tr([html.Th(col) for col in ["Type", "Generated", "Detected", "IsValid"]])]
        rows = []

        for row in table_data:
            is_valid = row[3]
            cell_color = "green" if is_valid else "red"
            rows.append(
                html.Tr(
                    [
                        html.Td(row[0]),
                        html.Td(row[1]),
                        html.Td(row[2]),
                        html.Td(row[3], style={"backgroundColor": cell_color})
                    ]
                )
            )

        return header + rows

    def validate_transaction(self, transaction_type, generated, detected):
        if transaction_type == "LT":
            return generated == 2 * detected
        elif transaction_type == "VT":
            return generated == 2 * detected
        elif transaction_type == "SL":
            return generated == detected
        return True

    def stop(self):
        self.kafka_thread_transactions.stop()
        self.kafka_thread_frauds.stop()

    def run(self):
        self.app.run_server(debug=True)


if __name__ == "__main__":
    monitor = TransactionMonitor()
    try:
        monitor.run()
    except KeyboardInterrupt:
        monitor.stop()
        print("Stopped monitoring.")
