from itertools import combinations
import logging

def apriori_algorithm(transactions, min_support, window_size):
    """Extract frequent itemsets from a list of transactions using the Apriori algorithm, focusing only on individual items."""
    item_count = {}
    for transaction in transactions:
        for item in transaction:
            if item in item_count:
                item_count[item] += 1
            else:
                item_count[item] = 1

    # Generate frequent 1-itemsets based on the min_support
    frequent_itemsets = [item for item, count in item_count.items() if count / len(transactions) >= min_support]

    # Logging for debug visibility
    logging.info("Frequent Itemsets Apriori Algorithm:")
    for itemset in frequent_itemsets:
        logging.info(itemset)

    return frequent_itemsets

def sliding_window(stream_data, window_size):
    """Generator to yield sliding windows of data."""
    for i in range(len(stream_data) - window_size + 1):
        yield stream_data[i:i + window_size]

