FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY kafka-producer.py .
COPY util.py .
CMD ["python", "kafka-producer.py"]
