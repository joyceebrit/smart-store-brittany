import pandas as pd
import sqlite3
import pathlib
import sys

# For local imports, temporarily add project root to sys.path
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Constants
DW_DIR = pathlib.Path("data").joinpath("dw")
DB_PATH = DW_DIR.joinpath("smart_sales.db")
PREPARED_DATA_DIR = pathlib.Path("data").joinpath("prepared")

def create_schema(cursor: sqlite3.Cursor) -> None:
    """ Create tables in the data warehouse if they don't exist."""

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INTEGER PRIMARY KEY,
            name TEXT,
            region TEXT,
            join_date TEXT,
            loyalty_points INTEGER,
            customer_segment TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            product_id INTEGER PRIMARY KEY,
            product_name TEXT,
            category TEXT,
            unit_price INTEGER,
            stock_quantity INTEGER,
            supplier TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sales (
            sale_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            product_id INTEGER,
            store_id INTEGER,
            campaign_id INTEGER,
            sale_amount REAL,
            sale_date TEXT,
            bonus_points INTEGER,
            payment_type TEXT,
            FOREIGN KEY (customer_id) REFERENCES customer (customer_id),
            FOREIGN KEY (product_id) REFERENCES product (product_id)
        )
    """)

    cursor.execute("""
        CREATE TABLE stores (
            store_id INTEGER PRIMARY KEY,
            store_name TEXT NOT NULL,
            location TEXT,
            state TEXT,
            country TEXT,
            store_type TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE campaigns (
            campaign_id INTEGER PRIMARY KEY,
            campaign_name TEXT NOT NULL,
            start_date DATE,
            end_date DATE,
            channel TEXT,
            budget_usd REAL
        )
    """)

def delete_existing_records(cursor: sqlite3.Cursor) -> None:
    """Delete all existing records from the customers, products, and sales tables."""
    cursor.execute("DELETE FROM customers")
    cursor.execute("DELETE FROM products")
    cursor.execute("DELETE FROM sales")

def insert_customers(customers_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """Insert customer data into the customers table."""
    customers_df.to_sql("customers", cursor.connection, if_exists="append", index=False)

def insert_products(products_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """Insert product data into the products table."""
    products_df.to_sql("products", cursor.connection, if_exists="append", index=False)

def insert_sales(sales_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """Insert sales data into the sales table."""
    sales_df.to_sql("sales", cursor.connection, if_exists="append", index=False)

def load_data_to_db() -> None:
    try:
        # Connect to SQLite – will create the file if it doesn't exist
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Create schema and clear existing records
        create_schema(cursor)
        delete_existing_records(cursor)

        # Load prepared data using pandas
        customers_df = pd.read_csv(PREPARED_DATA_DIR.joinpath("customers_data_prepared.csv"))
        products_df = pd.read_csv(PREPARED_DATA_DIR.joinpath("products_data_prepared.csv"))
        sales_df = pd.read_csv(PREPARED_DATA_DIR.joinpath("sales_data_prepared.csv"))

        # Insert data into the database
        insert_customers(customers_df, cursor)
        insert_products(products_df, cursor)
        insert_sales(sales_df, cursor)

        conn.commit()
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    load_data_to_db()