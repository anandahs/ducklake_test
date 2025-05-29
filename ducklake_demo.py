import duckdb
import os
from dotenv import load_dotenv

def setup_ducklake():
    # Load environment variables
    load_dotenv()
    
    # AWS credentials from .env file
    aws_access_key = os.getenv("AWS_ACCESS_KEY")
    aws_secret_key = os.getenv("AWS_SECRET_KEY")
    aws_region = os.getenv("AWS_REGION", "us-east-1")
    
    # Connect to DuckDB
    conn = duckdb.connect(database=':memory:')
    
    # Install DuckLake extension
    conn.execute("INSTALL ducklake")
    conn.execute("LOAD ducklake")
    
    # Configure AWS credentials for DuckLake
    conn.execute(f"SET s3_access_key_id='{aws_access_key}'")
    conn.execute(f"SET s3_secret_access_key='{aws_secret_key}'")
    
    print("DuckLake extension installed and configured successfully")
    
    return conn

def create_tables():
    # Get DuckDB connection with DuckLake configured
    conn = setup_ducklake()
    bucket_name = "ananda-ducklake-bucket"
    
    try:
        # Attach DuckLake database
        conn.execute(f"ATTACH 'ducklake:metadata.ducklake' AS my_ducklake (DATA_PATH 's3://{bucket_name}/data');")
        
        # Use the DuckLake database
        conn.execute('USE my_ducklake;')
        
        # Create customer table
        conn.execute('''
            CREATE TABLE customer(
                customer_id INTEGER,
                name VARCHAR,
                email VARCHAR,
                address VARCHAR
            );
        ''')
        
        # Insert sample data into customer table
        conn.execute('''
            INSERT INTO customer VALUES 
            (1, 'John Doe', 'john@example.com', '123 Main St'),
            (2, 'Jane Smith', 'jane@example.com', '456 Oak Ave'),
            (3, 'Bob Johnson', 'bob@example.com', '789 Pine Rd');
        ''')
        
        # Create inventory table
        conn.execute('''
            CREATE TABLE inventory(
                product_id INTEGER,
                product_name VARCHAR,
                quantity INTEGER,
                price DECIMAL(10,2)
            );
        ''')
        
        # Insert sample data into inventory table
        conn.execute('''
            INSERT INTO inventory VALUES 
            (101, 'Laptop', 50, 999.99),
            (102, 'Smartphone', 100, 499.99),
            (103, 'Tablet', 75, 299.99),
            (104, 'Headphones', 200, 99.99);
        ''')
        
        # Query data to verify
        customer_result = conn.execute('SELECT * FROM customer;').fetchall()
        inventory_result = conn.execute('SELECT * FROM inventory;').fetchall()
        
        print("Customer table created with sample data:")
        for row in customer_result:
            print(row)
            
        print("\nInventory table created with sample data:")
        for row in inventory_result:
            print(row)
            
        return conn
        
    except Exception as e:
        print(f"Error creating tables: {e}")
        return None

def main():
    print("Creating customer and inventory tables in DuckLake...")
    create_tables()

if __name__ == "__main__":
    main()