#!/bin/bash
python3 kafka-producer.py &

python3 flink.py &

python3 kafka-receiver.py &
