import requests
import logging
import mysql.connector
from datetime import datetime, timedelta
from GetAccessToken import get_access_token
import numpy as np
import pandas as pd
from statsmodels.tsa.seasonal import STL
from scipy.stats import norm
import yaml
from flask import Flask, request, jsonify
import os
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Load configuration
with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

# Substitute environment variables
config['amazon_api']['access_key'] = os.getenv('ACCESS_KEY')
config['amazon_api']['secret_key'] = os.getenv('SECRET_KEY')
config['amazon_api']['seller_id'] = os.getenv('SELLER_ID')
config['amazon_api']['marketplace_id'] = os.getenv('MARKETPLACE_ID')
config['amazon_api']['client_id'] = os.getenv('CLIENT_ID')
config['amazon_api']['client_secret'] = os.getenv('CLIENT_SECRET')
config['amazon_api']['refresh_token'] = os.getenv('REFRESH_TOKEN')

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
    df = pd.DataFrame(data, columns=['product_id', 'order_id', 'sale_date', 'sales_quantity', 'sales_price', 'warehouse_location', 'batch_number', 'expiration_date'])

    # Ensure columns are numeric
    df['sales_quantity'] = pd.to_numeric(df['sales_quantity'], errors='coerce')
    df['sales_price'] = pd.to_numeric(df['sales_price'], errors='coerce')

    # Handle missing values explicitly
    df = df.assign(
        sales_quantity=df['sales_quantity'].fillna(0),
        sales_price=df['sales_price'].fillna(0),
        warehouse_location=df['warehouse_location'].fillna('N/A'),
        batch_number=df['batch_number'].fillna('N/A'),
        expiration_date=df['expiration_date'].fillna(pd.NaT)
    )

    # Remove duplicates
    df.drop_duplicates(inplace=True)

    # Log intermediate data for debugging
    logging.debug(f"Data after cleaning and forward fill:\n{df}")

    # Convert data types to appropriate formats
    df['sales_quantity'] = df['sales_quantity'].astype(int)
    df['sales_price'] = df['sales_price'].astype(float)
    df['sale_date'] = pd.to_datetime(df['sale_date'])
    df['expiration_date'] = pd.to_datetime(df['expiration_date'], errors='coerce')

    # Summary statistics before outlier removal
    logging.debug(f"Summary statistics before outlier removal:\n{df.describe()}")

    # Additional outlier detection using IQR method (higher threshold)
    for column in ['sales_quantity', 'sales_price']:
        Q1 = df[column].quantile(0.25)
        Q3 = df[column].quantile(0.75)
        IQR = Q3 - Q1
        df = df[(df[column] >= (Q1 - 10 * IQR)) & (df[column] <= (Q3 + 10 * IQR))]  # Very high threshold
        logging.debug(f"Data after IQR outlier removal for {column}:\n{df}")

    # Summary statistics after outlier removal
    logging.debug(f"Summary statistics after outlier removal:\n{df.describe()}")
    logging.debug(f"Data after outlier removal:\n{df}")

    return df

def calculate_ema(df, span):
    """Calculate the Exponential Moving Average for sales quantities."""
    df['ema_sales_quantity'] = df['sales_quantity'].ewm(span=span, adjust=False).mean()
    logging.debug(f"Data with EMA:\n{df}")
    return df

def apply_stl_decomposition(df, period):
    """Apply Seasonal Decomposition of Time Series (STL) to adjust for seasonality."""
    stl = STL(df['sales_quantity'], period=period)
    result = stl.fit()
    df['seasonal'] = result.seasonal
    df['trend'] = result.trend
    df['residual'] = result.resid
    logging.debug(f"Data after STL Decomposition:\n{df}")
    return df

def calculate_safety_stock(daily_demand, lead_time, service_level=0.95):
    """Calculate safety stock based on demand variability and service level."""
    demand_std_dev = np.std(daily_demand)
    z_score = norm.ppf(service_level)
    safety_stock = z_score * demand_std_dev * np.sqrt(lead_time)
    return safety_stock

def calculate_reorder_point(df, lead_time, safety_stock):
    """Calculate reorder point based on sales velocity, lead time, and safety stock."""
    df['reorder_point'] = (df['ema_sales_quantity'] * lead_time) + safety_stock
    logging.debug(f"Data with Reorder Points:\n{df}")
    return df

def fetch_order_items(order_id, headers):
    endpoint = f"https://sellingpartnerapi-eu.amazon.com/orders/v0/orders/{order_id}/orderItems"
    response = requests.get(endpoint, headers=headers)
    if response.status_code == 200:
        data = response.json()
        logging.debug(f"Order Items for {order_id}: {data}")
        return data.get('payload', {}).get('OrderItems', [])
    else:
        logging.error(f"Failed to fetch order items for {order_id}. Status code: {response.status_code}")
        return []

def fetch_sales_data():
    logging.info("Fetching sales data...")
    access_token = get_access_token()
    logging.debug(f"Access Token: {access_token}")
    
    headers = {
        'x-amz-access-token': access_token,
        'x-amz-date': datetime.utcnow().strftime('%Y%m%dT%H%M%SZ'),
        'Content-Type': 'application/json',
    }
    endpoint = "https://sellingpartnerapi-eu.amazon.com/orders/v0/orders"
    params = {
        'MarketplaceIds': config['amazon_api']['marketplace_id'],
        'CreatedAfter': (datetime.now() - timedelta(days=1)).isoformat(),
        'OrderStatuses': 'Pending,PendingAvailability,Unshipped,PartiallyShipped,Shipped,InvoiceUnconfirmed'
    }
    
    logging.debug(f"Request Headers: {headers}")
    logging.debug(f"Request Params: {params}")

    response = requests.get(endpoint, headers=headers, params=params)
    data = response.json()
    
    logging.debug(f"Response Status Code: {response.status_code}")
    logging.debug(f"Response Data: {data}")

    sales_data = []
    if 'payload' in data and 'Orders' in data['payload']:
        for order in data['payload']['Orders']:
            logging.debug(f"Order Keys: {order.keys()}")
            order_id = order['AmazonOrderId']
            sale_date = order['PurchaseDate']
            sale_date = datetime.strptime(sale_date, "%Y-%m-%dT%H:%M:%SZ").date()
            
            order_items = fetch_order_items(order_id, headers)
            for item in order_items:
                logging.debug(f"Processing item: {item}")
                product_id = item['ASIN']
                sales_quantity = item['QuantityOrdered']
                sales_price = item.get('ItemPrice', {}).get('Amount')
                
                # Fetch additional fields: warehouse_location, batch_number, expiration_date
                warehouse_location = item.get('WarehouseLocation', 'N/A')
                batch_number = item.get('BatchNumber', 'N/A')
                expiration_date = item.get('ExpirationDate')

                if sales_price is not None:
                    sales_data.append((product_id, order_id, sale_date, sales_quantity, sales_price, warehouse_location, batch_number, expiration_date))
                else:
                    logging.warning(f"Missing 'ItemPrice' for item: {item}")

    if not sales_data:
        logging.warning("No sales data to process.")
        return []

    return sales_data

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
        'MarketplaceIds': config['amazon_api']['marketplace_id'],
    }
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
        if item[3] is None or item[4] is None:  # Index 3: sales_quantity, Index 4: sales_price
            integrity_issues.append(item)
    
    if integrity_issues:
        logging.error(f"Integrity issues found in data: {integrity_issues}")
        send_alert("Data integrity issue detected in sales data")
        return False
    
    logging.info("Data integrity check passed.")
    return True

def send_alert(message):
    """Send an alert for data issues."""
    # Implement alerting logic, e.g., send an email or a Slack message
    pass

# Fetch sales and inventory data and perform integrity check
sales_data = fetch_sales_data()
inventory_data = fetch_inventory_data()

if sales_data:
    if check_data_integrity(sales_data):
        logging.info("Proceeding with sales data processing...")
        cleaned_data = clean_and_validate_data(sales_data)
        ema_data = calculate_ema(cleaned_data, span=config['data_processing']['ema_span'])  # Use config value
        stl_data = apply_stl_decomposition(ema_data, period=config['data_processing']['stl_period'])  # Use config value
        daily_demand = stl_data['residual']
        safety_stock = calculate_safety_stock(daily_demand, lead_time=config['data_processing']['lead_time'], service_level=config['data_processing']['service_level'])  # Use config values
        final_data = calculate_reorder_point(stl_data, lead_time=config['data_processing']['lead_time'], safety_stock=safety_stock)  # Use config values
        logging.info("Sales data processing complete.")
        # Save final_data to database or use it for further analysis

        # Convert DataFrame to list of tuples
        records = [tuple(x) for x in final_data.values]
        cursor.executemany("""
            INSERT INTO inventory_data (product_id, order_id, sale_date, sales_quantity, sales_price, warehouse_location, batch_number, expiration_date, ema_sales_quantity, seasonal, trend, residual, reorder_point)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                sale_date = VALUES(sale_date),
                sales_quantity = VALUES(sales_quantity),
                sales_price = VALUES(sales_price),
                warehouse_location = VALUES(warehouse_location),
                batch_number = VALUES(batch_number),
                expiration_date = VALUES(expiration_date),
                ema_sales_quantity = VALUES(ema_sales_quantity),
                seasonal = VALUES(seasonal),
                trend = VALUES(trend),
                residual = VALUES(residual),
                reorder_point = VALUES(reorder_point)
        """, records)
        db.commit()
        logging.info("Inventory data updated successfully.")
    else:
        logging.error("Data integrity check failed. Aborting sales data processing.")
else:
    logging.error("No sales data fetched. Aborting sales data processing.")

if inventory_data:
    store_inventory_data(inventory_data)
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
            # Assume the webhook sends sales data in the required format
            cleaned_data = clean_and_validate_data(data)
            ema_data = calculate_ema(cleaned_data, span=config['data_processing']['ema_span'])
            stl_data = apply_stl_decomposition(ema_data, period=config['data_processing']['stl_period'])
            daily_demand = stl_data['residual']
            safety_stock = calculate_safety_stock(daily_demand, lead_time=config['data_processing']['lead_time'], service_level=config['data_processing']['service_level'])
            final_data = calculate_reorder_point(stl_data, lead_time=config['data_processing']['lead_time'], safety_stock=safety_stock)
            logging.info("Webhook data processing complete.")

            # Save final_data to database
            records = [tuple(x) for x in final_data.values]
            cursor.executemany("""
                INSERT INTO inventory_data (product_id, order_id, sale_date, sales_quantity, sales_price, warehouse_location, batch_number, expiration_date, ema_sales_quantity, seasonal, trend, residual, reorder_point)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    sale_date = VALUES(sale_date),
                    sales_quantity = VALUES(sales_quantity),
                    sales_price = VALUES(sales_price),
                    warehouse_location = VALUES(warehouse_location),
                    batch_number = VALUES(batch_number),
                    expiration_date = VALUES(expiration_date),
                    ema_sales_quantity = VALUES(ema_sales_quantity),
                    seasonal = VALUES(seasonal),
                    trend = VALUES(trend),
                    residual = VALUES(residual),
                    reorder_point = VALUES(reorder_point)
            """, records)
            db.commit()
            logging.info("Webhook inventory data updated successfully.")
            return jsonify({'status': 'success'}), 200
        else:
            logging.warning("Empty data received from webhook.")
            return jsonify({'status': 'no data'}), 400
    except Exception as e:
        logging.error(f"Error processing webhook data: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    app.run(port=config['webhook']['port'])