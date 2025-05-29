import boto3
from botocore.exceptions import ClientError
import os
from dotenv import load_dotenv
import duckdb

def create_bucket_and_folder():
    # Load environment variables
    # AWS credentials from .env file
    aws_access_key = os.getenv("AWS_ACCESS_KEY")
    aws_secret_key = os.getenv("AWS_SECRET_KEY")
    
    # Initialize S3 client
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )
    
    bucket_name = "ananda-ducklake-bucket"
    
    # Check if bucket already exists
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket {bucket_name} already exists")
    except ClientError as e:
        # If bucket doesn't exist, create it
        if e.response['Error']['Code'] == '404':
            try:
                s3_client.create_bucket(Bucket=bucket_name)
                print(f"Bucket {bucket_name} created successfully")
            except ClientError as e:
                print(f"Error creating bucket: {e}")
                return False
        else:
            print(f"Error checking bucket: {e}")
            return False
    
    # Check if 'data/' folder exists and create if it doesn't
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix="data/",
            MaxKeys=1
        )
        if 'Contents' in response and any(obj['Key'] == 'data/' for obj in response['Contents']):
            print("'data/' folder already exists")
        else:
            s3_client.put_object(Bucket=bucket_name, Key="data/")
            print("'data/' folder created successfully")
    except ClientError as e:
        print(f"Error creating 'data/' folder: {e}")
        return False
    
    return True

def setup_ducklake():
    # Load environment variables
    load_dotenv()
    
    # AWS credentials from .env file
    aws_access_key = os.getenv("AWS_ACCESS_KEY")
    aws_secret_key = os.getenv("AWS_SECRET_KEY")
    
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

def create_ducklake_tables():
    # Get DuckDB connection with DuckLake configured
    conn = setup_ducklake()
    bucket_name = "ananda-ducklake-bucket"
    
    try:
        # Attach DuckLake database
        conn.execute(f"ATTACH 'ducklake:metadata.ducklake' AS my_ducklake (DATA_PATH 's3://{bucket_name}/data');")
        
        # Use the DuckLake database
        conn.execute('USE my_ducklake;')
        
        # Create schema
        conn.execute('''drop table if exists customer;''')
        conn.execute('''drop table if exists inventory;''')
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

    # Load environment variables
    load_dotenv()
    # Check if AWS credentials are set
    if not os.getenv("AWS_ACCESS_KEY") or not os.getenv("AWS_SECRET_KEY"):
        print("Please set AWS_ACCESS_KEY and AWS_SECRET_KEY in your .env file")
        return
    # Create S3 bucket and folder
    if not create_bucket_and_folder():
        print("Failed to create S3 bucket and folder. Exiting setup.")
        return
    # Setup DuckLake extension
    create_ducklake_tables()
    print("DuckLake tables and data completed successfully.")


if __name__ == "__main__":
    main()