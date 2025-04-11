#!/bin/bash

# Start Zookeeper
echo "Starting Zookeeper..."
/home/maryam/kafka/bin/zookeeper-server-start.sh /home/maryam/kafka/config/zookeeper.properties &

# Start Kafka Server
echo "Starting Kafka Server..."
/home/maryam/kafka/bin/kafka-server-start.sh /home/maryam/kafka/config/server.properties &

# Start Kafka Connect (if needed)
echo "Starting Kafka Connect..."
/home/maryam/kafka/bin/connect-standalone.sh /home/maryam/kafka/config/connect-standalone.properties /home/maryam/kafka/config/connect-file-source.properties &


echo "Starting Producer Application..."
python3 /home/maryam/kafka/assignment_3_kafka/producer.py &

echo "Starting Consumer Applications..."
python3 /home/maryam/kafka/assignment_3_kafka/consumer1.py &
python3 /home/maryam/kafka/assignment_3_kafka/consumer2.py &
python3 /home/maryam/kafka/assignment_3_kafka/consumer3.py &

echo "Project execution started successfully!"

