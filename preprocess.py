import pandas as pd
import json
from html import unescape

def cleaned_text(text):
    if pd.isna(text):
        return ""
    return unescape(text).replace("<br>", " ").replace("<span>", "").replace("</span>", "").strip()

def getprice(price_str):
    try:
        return float(price_str.strip('$'))
    except:
        return None

input_file_path = "C:/dataframe/Sampled_Amazon_Meta.json"
output_file_path = 'C:/dataframe/preprocessed_data.json'

chunk_size = 10000  # Adjust the chunk size as needed

try:
    with open(output_file_path, 'a', encoding='utf-8') as output_file:
        for chunk in pd.read_json(input_file_path, lines=True, chunksize=chunk_size):
            processed_data = []
            for index, item in chunk.iterrows():
                title = cleaned_text(item.get('title', ''))
                description = cleaned_text(' '.join(item.get('description', [])))
                price = getprice(item.get('price', ''))
                category = item.get('category', [''])[0] if item.get('category') else ''
                also_buy = item.get('also_buy', [])
                date = item.get('date', '')
                # Add additional columns here
                other_columns = {
                    'brand': item.get('brand', ''),
                    'manufacturer': item.get('manufacturer', ''),
                    'reviews': item.get('reviews', ''),
                    # Add more columns as needed
                }
                columns = {'title': title, 'description': description, 'price': price, 'category': category, 'also_buy': also_buy, 'date': date, **other_columns}
                processed_data.append(columns)

            if processed_data:  # Check if processed_data is not empty before creating DataFrame
                df = pd.DataFrame(processed_data)
                df = df[['title', 'description', 'price', 'category', 'also_buy', 'date', 'brand', 'manufacturer', 'reviews']]  # Include additional columns here
                df.to_json(output_file, orient='records', lines=True)
            
    print("Data preprocessing completed successfully.")

except Exception as e:
    print("An error occurred during data preprocessing:", e)
