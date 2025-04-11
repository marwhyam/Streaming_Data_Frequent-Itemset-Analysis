# third.py

from kafka import KafkaProducer
import json

def send_data(data):
    """ Send data to a Kafka topic. """
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    # Example: Assuming 'data' is a dictionary containing relevant information
    producer.send('amazon_data_topic', value=data)
    producer.flush()
    print("Data sent successfully!")

if __name__ == "__main__":
    data = {
        'product_name': 'Product X',
        'description': 'This is a great product!',
        'price': 99.99
    }
    send_data(data)

