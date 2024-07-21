import unittest
import mysql.connector
from CollectInventoryData import fetch_inventory_data
import yaml
import os

# Load configuration
with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

# Connect to MySQL database
db = mysql.connector.connect(
    host=config['database']['host'],
    user=config['database']['user'],
    password=config['database']['password'],
    database=config['database']['database']
)
cursor = db.cursor()

class TestInventoryData(unittest.TestCase):

    def setUp(self):
        # Ensure database connection is available
        self.db = db
        self.cursor = cursor

    def tearDown(self):
        # Close the database connection
        self.cursor.close()
        self.db.close()

    def test_fetch_inventory_data(self):
        # Fetch the inventory data using the existing function
        inventory_data = fetch_inventory_data()

        # Check that data is not empty
        self.assertIsNotNone(inventory_data)
        self.assertTrue(len(inventory_data) > 0, "Inventory data should not be empty")

        # Check that required fields are present
        for item in inventory_data:
            self.assertIn('product_id', item)
            self.assertIn('warehouse_location', item)
            self.assertIn('available_quantity', item)

        print("Fetched inventory data:", inventory_data)

    def test_db_inventory_data(self):
        # Query the database to check available stock
        query = "SELECT product_id, current_stock FROM inventory_data WHERE current_stock > 0"
        self.cursor.execute(query)
        result = self.cursor.fetchall()

        # Check that result is not empty
        self.assertIsNotNone(result)
        self.assertTrue(len(result) > 0, "Database should return items with available stock")

        print("Current available stock from database:", result)

if __name__ == '__main__':
    unittest.main()
