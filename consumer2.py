from kafka import KafkaConsumer
import json
import logging
from pymongo import MongoClient
from pcy import pcy_algorithm, sliding_window

# Setup logging for better debug visibility
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Setup MongoDB connection
client = MongoClient('localhost', 27017)
db = client['pcy_results']  # Database where the results will be stored
collection = db['frequent_itemsets']  # Collection to store frequent itemsets

def consume_data(min_support, window_size, hash_table_size):
    """ Consume data from a Kafka topic, process using PCY algorithm, and store results in MongoDB. """
    consumer = KafkaConsumer(
        'amazon_data_topic',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    for message in consumer:
        stream_data = message.value  # Extract the message value
        if 'also_buy' in stream_data and isinstance(stream_data['also_buy'], list):
            frequent_itemsets, frequent_pairs = pcy_algorithm(stream_data['also_buy'], min_support, window_size, hash_table_size)
            logging.info("Frequent Itemsets using PCY Algorithm:")
            logging.info(f"Frequent Itemsets: {frequent_itemsets}")
            logging.info(f"Frequent Pairs: {frequent_pairs}")

            # Store results in MongoDB
            document = {
                'frequent_itemsets': frequent_itemsets,
                'frequent_pairs': frequent_pairs,
                'timestamp': message.timestamp
            }
            collection.insert_one(document)
        else:
            logging.warning("Missing 'also_buy' or incorrect data format in message.")

if __name__ == "__main__":
    min_support = 0.01  # Adjust min_support as needed for your dataset size
    window_size = 100
    hash_table_size = 10000  # Adjust hash_table_size as needed
    consume_data(min_support, window_size, hash_table_size)

