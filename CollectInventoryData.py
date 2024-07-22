import requests
import logging
import mysql.connector
from datetime import datetime
import numpy as np
import pandas as pd
import yaml
from flask import Flask, request, jsonify
import os
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv
from GetAccessToken import get_access_token

# Load environment variables from .env file
load_dotenv()
logging.info("Loaded environment variables from .env file")

# Log the environment variables
logging.info(f"ACCESS_KEY: {os.getenv('ACCESS_KEY')}")
logging.info(f"SECRET_KEY: {os.getenv('SECRET_KEY')}")
logging.info(f"SELLER_ID: {os.getenv('SELLER_ID')}")
logging.info(f"MARKETPLACE_ID: {os.getenv('MARKETPLACE_ID')}")
logging.info(f"CLIENT_ID: {os.getenv('CLIENT_ID')}")
logging.info(f"CLIENT_SECRET: {os.getenv('CLIENT_SECRET')}")
logging.info(f"REFRESH_TOKEN: {os.getenv('REFRESH_TOKEN')}")

# Load configuration
with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)
logging.info("Loaded configuration from config.yaml")

# Substitute environment variables
config['amazon_api']['access_key'] = os.getenv('ACCESS_KEY')
config['amazon_api']['secret_key'] = os.getenv('SECRET_KEY')
config['amazon_api']['seller_id'] = os.getenv('SELLER_ID')
config['amazon_api']['marketplace_id'] = os.getenv('MARKETPLACE_ID')
config['amazon_api']['client_id'] = os.getenv('CLIENT_ID')
config['amazon_api']['client_secret'] = os.getenv('CLIENT_SECRET')
config['amazon_api']['refresh_token'] = os.getenv('REFRESH_TOKEN')

# Verify configuration
logging.info(f"Final configuration: {config['amazon_api']}")

# Ensure log directory exists
log_file_path = os.path.expanduser('~/PlaneHealth/logs/inventory.log')
log_dir = os.path.dirname(log_file_path)
os.makedirs(log_dir, exist_ok=True)

# Set up logging
logging_config = config['logging']
handlers = []
if 'console' in logging_config['handlers']:
    handlers.append(logging.StreamHandler())

if 'file' in logging_config['handlers']:
    file_handler = RotatingFileHandler(
        log_file_path,
        maxBytes=logging_config['file']['maxBytes'],
        backupCount=logging_config['file']['backupCount']
    )
    handlers.append(file_handler)

logging.basicConfig(
    level=logging_config['level'],
    format=logging_config['format'],
    handlers=handlers
)

# Connect to MySQL database
db = mysql.connector.connect(
    host=config['database']['host'],
    user=config['database']['user'],
    password=config['database']['password'],
    database=config['database']['database']
)
cursor = db.cursor()

def clean_and_validate_data(data):
    df = pd.DataFrame(data, columns=['product_id', 'warehouse_location', 'current_stock', 'batch_number', 'expiration_date'])

    # Ensure columns are numeric
    df['current_stock'] = pd.to_numeric(df['current_stock'], errors='coerce')

    # Handle missing values explicitly
    df = df.assign(
        current_stock=df['current_stock'].fillna(0),
        warehouse_location=df['warehouse_location'].fillna('N/A'),
        batch_number=df['batch_number'].fillna('N/A'),
        expiration_date=df['expiration_date'].fillna(pd.NaT)
    )

    # Remove duplicates
    df.drop_duplicates(inplace=True)

    # Log intermediate data for debugging
    logging.debug(f"Data after cleaning and forward fill:\n{df}")

    # Convert data types to appropriate formats
    df['current_stock'] = df['current_stock'].astype(int)
    df['expiration_date'] = pd.to_datetime(df['expiration_date'], errors='coerce')

    # Summary statistics before outlier removal
    logging.debug(f"Summary statistics before outlier removal:\n{df.describe()}")

    # Additional outlier detection using IQR method (higher threshold)
    for column in ['current_stock']:
        Q1 = df[column].quantile(0.25)
        Q3 = df[column].quantile(0.75)
        IQR = Q3 - Q1
        df = df[(df[column] >= (Q1 - 10 * IQR)) & (df[column] <= (Q3 + 10 * IQR))]  # Very high threshold
        logging.debug(f"Data after IQR outlier removal for {column}:\n{df}")

    # Summary statistics after outlier removal
    logging.debug(f"Summary statistics after outlier removal:\n{df.describe()}")
    logging.debug(f"Data after outlier removal:\n{df}")

    return df

def fetch_inventory_data():
    logging.info("Fetching inventory data...")
    access_token = get_access_token()
    headers = {
        'x-amz-access-token': access_token,
        'x-amz-date': datetime.utcnow().strftime('%Y%m%dT%H%M%SZ'),
        'Content-Type': 'application/json',
    }
    endpoint = "https://sellingpartnerapi-eu.amazon.com/fba/inventory/v1/summaries"
    params = {
        'details': 'true',
        'granularityType': 'Marketplace',
        'granularityId': config['amazon_api']['marketplace_id'],
        'marketplaceIds': config['amazon_api']['marketplace_id'],
    }
    
    logging.debug(f"Request URL: {endpoint}")
    logging.debug(f"Request Headers: {headers}")
    logging.debug(f"Request Params: {params}")
    
    response = requests.get(endpoint, headers=headers, params=params)
    logging.debug(f"Inventory Data Fetch Response Status Code: {response.status_code}")
    logging.debug(f"Inventory Data Fetch Response: {response.text}")
    
    if response.status_code == 200:
        data = response.json()
        inventory_data = []
        if 'payload' in data and 'InventorySummaries' in data['payload']:
            for item in data['payload']['InventorySummaries']:
                product_id = item['asin']
                warehouse_location = item['fulfillmentCenterId']
                available_quantity = item['totalSupplyQuantity']['quantity']
                inventory_data.append((product_id, warehouse_location, available_quantity))
        return inventory_data
    else:
        logging.error(f"Failed to fetch inventory data. Status code: {response.status_code}")
        error_data = response.json()
        if 'errors' in error_data:
            for error in error_data['errors']:
                logging.error(f"Error Code: {error['code']}, Message: {error['message']}, Details: {error['details']}")
        return []

def store_inventory_data(data):
    if not data:
        logging.warning("No inventory data to store.")
        return
    records = [(x[0], x[1], x[2]) for x in data]  # Extract product_id, warehouse_location, available_quantity
    cursor.executemany("""
        INSERT INTO inventory_data (product_id, warehouse_location, current_stock)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            warehouse_location = VALUES(warehouse_location),
            current_stock = VALUES(current_stock)
    """, records)
    db.commit()
    logging.info("Inventory data updated successfully.")

def check_data_integrity(data):
    """Check for data integrity issues."""
    if not data:
        logging.error("No data available to check for integrity.")
        return False
    
    integrity_issues = []
    for item in data:
        if item[2] is None:  # Index 2: current_stock
            integrity_issues.append(item)
    
    if integrity_issues:
        logging.error(f"Integrity issues found in data: {integrity_issues}")
        send_alert("Data integrity issue detected in inventory data")
        return False
    
    logging.info("Data integrity check passed.")
    return True

def send_alert(message):
    """Send an alert for data issues."""
    # Implement alerting logic, e.g., send an email or a Slack message
    pass

# Fetch inventory data and perform integrity check
inventory_data = fetch_inventory_data()

if inventory_data:
    if check_data_integrity(inventory_data):
        logging.info("Proceeding with inventory data processing...")
        cleaned_data = clean_and_validate_data(inventory_data)
        store_inventory_data(cleaned_data)
        logging.info("Inventory data processing complete.")
    else:
        logging.error("Data integrity check failed. Aborting inventory data processing.")
else:
    logging.error("No inventory data fetched. Aborting inventory data processing.")

# Webhook Integration for Real-Time Updates
app = Flask(__name__)

@app.route('/webhook', methods=['POST'])
def webhook():
    logging.info("Webhook received data")
    try:
        data = request.json
        if data:
            logging.debug(f"Webhook data: {data}")
            # Assume the webhook sends inventory data in the required format
            cleaned_data = clean_and_validate_data(data)
            store_inventory_data(cleaned_data)
            logging.info("Webhook data processing complete.")
            return jsonify({'status': 'success'}), 200
        else:
            logging.warning("Empty data received from webhook.")
            return jsonify({'status': 'no data'}), 400
    except Exception as e:
        logging.error(f"Error processing webhook data: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    app.run(port=config['webhook']['port'])
