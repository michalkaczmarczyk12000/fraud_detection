#!/bin/bash
sleep 30
python3 kafka-producer.py &

python3 flink.py &

python3 kafka-receiver.py &
