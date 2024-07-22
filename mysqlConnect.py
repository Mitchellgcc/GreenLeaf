import mysql.connector
import yaml

# Load database configuration from config.yaml
with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)
db_config = config['database']

def main():
    try:
        conn = mysql.connector.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database']
        )
        cursor = conn.cursor()
        
        # Execute ALTER TABLE command
        alter_table_query = """
        ALTER TABLE inventory_data
        ADD COLUMN order_id VARCHAR(255),
        ADD COLUMN sale_date DATE,
        ADD COLUMN sales_quantity INT,
        ADD COLUMN sales_price FLOAT,
        ADD COLUMN ema_sales_quantity FLOAT,
        ADD COLUMN seasonal FLOAT,
        ADD COLUMN trend FLOAT,
        ADD COLUMN residual FLOAT,
        ADD COLUMN reorder_point FLOAT;
        """
        cursor.execute(alter_table_query)
        conn.commit()
        
        # Verify table schema
        cursor.execute("DESCRIBE inventory_data;")
        table_schema = cursor.fetchall()
        print("Updated table schema:")
        for row in table_schema:
            print(row)

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    main()
