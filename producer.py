# producer.py

import json
from kafka import KafkaProducer
import time

def json_serializer(data):
    """ Serialize JSON data for Kafka messaging. """
    return json.dumps(data).encode('utf-8')

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=json_serializer)

def produce_data(file_path):
    """ Read JSON data from a file and produce it to a Kafka topic. """
    with open(file_path, 'r') as file:
        for line in file:
            message = json.loads(line)
            producer.send('amazon_data_topic', value=message)
            time.sleep(0.01)  # simulating real-time streaming
            print(f"Sending data: {json.dumps(message, indent=4)}")

if __name__ == "__main__": 
    produce_data('/home/maryam/preprocessed_data.json')

