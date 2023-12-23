import requests
import base64
import json
import pandas as pd

DATA_URL = "https://raw.githubusercontent.com/hecrodjr/data-methods-project/main/dataset/datamethods-k0vtl-ke28jtmf-books_data.b64"
OUTPUT_FILE = "data_pipeline_output.csv"

def download_data(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        decoded_data = base64.b64decode(response.content).decode('utf-8')
        json_data = json.loads(decoded_data)
        books_data = json_data['books']
        return books_data
    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to download file. Error: {e}")

def standarize_case(df):
    df['title'] = df['title'].str.upper()
    df['author'] = df['author'].str.upper()

def clean_price_column(df):
    df['price'] = df['price'].replace('[\$,]', '', regex=True).astype(float)

def calculate_revenue(df):
    df['revenue'] = round(df['quantity'] * df['price'], 2)

def clean_data(data):
    df = pd.DataFrame(data)
    standarize_case(df)
    clean_price_column(df)
    calculate_revenue(df)
    return df

def save_to_csv(data, filename):
    data.to_csv(filename, index=False)

if __name__ == "__main__":
    data = download_data(DATA_URL)
    cleaned_data = clean_data(data)
    save_to_csv(cleaned_data, OUTPUT_FILE)
