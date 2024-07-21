import requests
import logging
import mysql.connector
from datetime import datetime, timedelta
from GetAccessToken import get_access_token
import numpy as np
import pandas as pd
from statsmodels.tsa.seasonal import STL
from sklearn.ensemble import RandomForestRegressor
import yaml
import os
from logging.handlers import RotatingFileHandler

# Load configuration
with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

# Ensure log directory exists
log_file_path = '/Users/georgemitchell/Library/Mobile Documents/com~apple~CloudDocs/New Business/Plane Health/Amazon (Cornwells)/Software/logs/sales.log'
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
    df = pd.DataFrame(data, columns=['product_id', 'order_id', 'sale_date', 'sales_quantity', 'sales_price'])

    # Ensure columns are numeric
    df['sales_quantity'] = pd.to_numeric(df['sales_quantity'], errors='coerce')
    df['sales_price'] = pd.to_numeric(df['sales_price'], errors='coerce')

    # Handle missing values explicitly
    df = df.fillna({'sales_quantity': 0, 'sales_price': 0})

    # Remove duplicates
    df.drop_duplicates(inplace=True)

    # Log intermediate data for debugging
    logging.debug(f"Data after cleaning and forward fill:\n{df}")

    # Convert data types to appropriate formats
    df['sales_quantity'] = df['sales_quantity'].astype(int)
    df['sales_price'] = df['sales_price'].astype(float)
    df['sale_date'] = pd.to_datetime(df['sale_date'])

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
    df.set_index('sale_date', inplace=True)
    stl = STL(df['sales_quantity'], period=period)
    result = stl.fit()
    df['seasonal'] = result.seasonal
    df['trend'] = result.trend
    df['residual'] = result.resid
    df.reset_index(inplace=True)
    logging.debug(f"Data after STL Decomposition:\n{df}")
    return df

def identify_slow_selling_items(df, threshold=1):
    """Identify slow-selling items using dynamic thresholds."""
    # Use a machine learning model for more sophisticated prediction (e.g., Random Forest)
    features = df[['ema_sales_quantity', 'seasonal', 'trend']]
    target = df['sales_quantity']
    model = RandomForestRegressor(n_estimators=100)
    model.fit(features, target)
    
    # Predict sales for the next period and identify slow-selling items
    df['predicted_sales'] = model.predict(features)
    df['slow_selling'] = df['predicted_sales'] < threshold
    slow_selling_items = df[df['slow_selling']].reset_index()
    return slow_selling_items

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
                sales_price = item.get('ItemPrice', {}).get('Amount', 0)  # Default to 0 if missing
                
                if sales_price is not None:
                    sales_data.append((product_id, order_id, sale_date, sales_quantity, sales_price))
                else:
                    logging.warning(f"Missing 'ItemPrice' for item: {item}")

    if not sales_data:
        logging.warning("No sales data to process.")
        return []

    return sales_data

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

def store_slow_selling_results(df):
    """Store the results in the database."""
    records = [tuple(x) for x in df[['product_id', 'predicted_sales']].values]
    cursor.executemany("""
        INSERT INTO slow_selling_items (product_id, predicted_sales)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE predicted_sales = VALUES(predicted_sales)
    """, records)
    db.commit()
    logging.info("Slow-selling items updated successfully.")

# Fetch sales data and perform integrity check
sales_data = fetch_sales_data()
if sales_data:
    if check_data_integrity(sales_data):
        logging.info("Proceeding with data processing...")
        cleaned_data = clean_and_validate_data(sales_data)
        ema_data = calculate_ema(cleaned_data, span=config['data_processing']['ema_span'])  # Use config span
        stl_data = apply_stl_decomposition(ema_data, period=config['data_processing']['stl_period'])  # Use config period
        slow_selling_items = identify_slow_selling_items(stl_data, threshold=1)
        if not slow_selling_items.empty:
            store_slow_selling_results(slow_selling_items)
        else:
            logging.info("No slow-selling items identified.")
        logging.info("Data processing complete.")
    else:
        logging.error("Data integrity check failed. Aborting data processing.")
else:
    logging.error("No sales data fetched. Aborting data processing.")