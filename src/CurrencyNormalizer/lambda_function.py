import json
import boto3
import csv
import io
import os
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime
import requests

# Initialize AWS clients
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# API endpoint and environment variable for the API key
EXCHANGE_API_URL = "http://api.exchangeratesapi.io/v1/latest"
EXCHANGE_API_KEY = os.getenv('EXCHANGE_API_KEY')


def lambda_handler(event, context):
    # Environment variables
    raw_bucket = os.getenv('RAW_PROPERTIES_BUCKET')
    processed_bucket = os.getenv('PROCESSED_PROPERTIES_BUCKET')
    table_name = os.getenv('PROPERTIES_TABLE_NAME')

    # Get the DynamoDB table
    table = dynamodb.Table(table_name)

    # Fetch the latest exchange rates
    exchange_rates = fetch_exchange_rates()

    # Process each record in the event
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        try:
            csv_data = read_csv_from_s3(bucket, key)
            processed_data = process_csv_data(csv_data, exchange_rates)
            upload_to_dynamodb(table, processed_data)
            new_key = 'processed_' + key
            write_processed_csv_to_s3(processed_bucket, new_key, processed_data)

        except Exception as e:
            print(f"Error processing file {key} from bucket {bucket}: {e}")

    return {
        'statusCode': 200,
        'body': json.dumps('Currency normalization complete!')
    }


def fetch_exchange_rates():
    try:
        response = requests.get(f"{EXCHANGE_API_URL}?access_key={EXCHANGE_API_KEY}&symbols=USD,CAD,EUR,GBP,AUD")
        response.raise_for_status()
        data = response.json()

        rates = data.get('rates', {})
        usd_rate = rates.get('USD')
        if usd_rate is None:
            raise ValueError("Missing exchange rate for USD")

        exchange_rates = {currency: Decimal(usd_rate) / Decimal(rate) for currency, rate in rates.items() if rate}
        return exchange_rates
    except Exception as e:
        print(f"Error fetching exchange rates: {e}")
        raise


def read_csv_from_s3(bucket, key):
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        lines = response['Body'].read().decode('utf-8').splitlines()
        csv_data = list(csv.DictReader(lines))
        return csv_data
    except Exception as e:
        print(f"Error reading file {key} from bucket {bucket}: {e}")
        raise


def process_csv_data(csv_data, exchange_rates):
    processed_data = []
    for row in csv_data:
        if 'price' in row and 'currency' in row:
            price = Decimal(row['price'])
            currency = row['currency']
            if currency in exchange_rates:
                row['price'] = str(convert_currency(price, exchange_rates[currency]))
                row['currency'] = 'USD'
            row['creationDate'] = datetime.utcnow().isoformat()
            processed_data.append(row)
    return processed_data


def convert_currency(price, exchange_rate):
    converted_price = price * exchange_rate
    return converted_price.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)  # Limit to 2 decimal places


def upload_to_dynamodb(table, items):
    with table.batch_writer() as batch:
        for item in items:
            try:
                item['zpid'] = str(item['zpid'])
                item['creationDate'] = str(item['creationDate'])
                batch.put_item(Item=item)
            except Exception as e:
                print(f"Error uploading item to DynamoDB: {e}")


def write_processed_csv_to_s3(bucket, key, data):
    try:
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
        s3.put_object(Bucket=bucket, Key=key, Body=output.getvalue().encode('utf-8'))
    except Exception as e:
        print(f"Error writing processed file to bucket {bucket} with key {key}: {e}")