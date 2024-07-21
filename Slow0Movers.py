import mysql.connector
import pandas as pd
import logging
from datetime import datetime
import os
from logging.handlers import RotatingFileHandler
import yaml

# Load configuration
with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

# Ensure log directory exists
log_file_path = os.path.expanduser('~/PlaneHealth/logs/slow_movers.log')
log_dir = os.path.dirname(log_file_path)
os.makedirs(log_dir, exist_ok=True)

# Set up logging
logging_config = config['logging']
handlers = [logging.StreamHandler()]

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
    database=config['database']['database']  # This should be greenleaf_db
)
cursor = db.cursor()

def fetch_combined_data():
    query = """
        SELECT i.product_id, i.current_stock AS available_quantity, COALESCE(SUM(i.sales_quantity), 0) AS total_sales_quantity
        FROM inventory_data i
        GROUP BY i.product_id, i.current_stock
    """
    cursor.execute(query)
    combined_data = cursor.fetchall()
    columns = ['product_id', 'available_quantity', 'total_sales_quantity']
    df = pd.DataFrame(combined_data, columns=columns)
    return df

def identify_slow_selling_items(df, days=30, threshold=1):
    df['average_daily_sales'] = df['total_sales_quantity'] / days
    df['is_slow_selling'] = df['average_daily_sales'] < threshold
    slow_selling_items = df[df['is_slow_selling'] & (df['available_quantity'] > 0)]
    return slow_selling_items

def store_slow_selling_items(df):
    records = [tuple(x) for x in df[['product_id', 'average_daily_sales', 'available_quantity']].values]
    cursor.executemany("""
        INSERT INTO slow_selling_items (product_id, average_daily_sales, available_quantity)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            average_daily_sales = VALUES(average_daily_sales),
            available_quantity = VALUES(available_quantity)
    """, records)
    db.commit()
    logging.info("Slow-selling items updated successfully.")

# Main process
combined_data = fetch_combined_data()
slow_selling_items = identify_slow_selling_items(combined_data)
if not slow_selling_items.empty:
    store_slow_selling_items(slow_selling_items)
else:
    logging.info("No slow-selling items identified.")
