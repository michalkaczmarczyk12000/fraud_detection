FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y \
    openjdk-11-jdk \
    || apt-get install -y \
    default-jdk

COPY . /app
RUN pip install -r requirements.txt
RUN chmod 777 flink-sql-connector-kafka-3.1.0-1.18.jar entrypoint.sh

CMD [ "./entrypoint.sh" ]
