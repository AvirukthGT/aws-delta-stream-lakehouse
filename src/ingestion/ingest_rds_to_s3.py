import os
import json
import boto3
import pandas as pd
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
S3_BUCKET = os.getenv("S3_RAW_BUCKET")
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
    "port": os.getenv("DB_PORT")
}
STATE_FILE = "ingestion_state.json"

def get_last_watermark(table_name):
    """
    Reads the last processed timestamp from a local JSON file.
    If no file exists, defaults to a safe past date (Full Load).
    """
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            state = json.load(f)
            return state.get(table_name, "2025-01-01 00:00:00")
    return "2025-01-01 00:00:00"

def update_watermark(table_name, new_timestamp):
    """
    Saves the new max timestamp to the state file.
    """
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            state = json.load(f)
    else:
        state = {}
    
    state[table_name] = str(new_timestamp)
    
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=4)

def ingest_table(table_name):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        watermark = get_last_watermark(table_name)
        print(f"{table_name}: Fetching changes since {watermark}...")
        
        # Read incremental data
        query = f"SELECT * FROM {table_name} WHERE updated_at > %s"
        df = pd.read_sql(query, conn, params=(watermark,))
        
        if not df.empty:
            # 1. Save to S3
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            file_name = f"bronze/{table_name}/{timestamp}.parquet"
            
            parquet_buffer = df.to_parquet(index=False, compression='snappy')
            
            s3 = boto3.client(
                's3',
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                region_name=os.getenv("AWS_REGION")
            )
            s3.put_object(Bucket=S3_BUCKET, Key=file_name, Body=parquet_buffer)
            
            # 2. Update Watermark (State)
            # We take the MAX(updated_at) from the data we just pulled
            max_date = df['updated_at'].max()
            update_watermark(table_name, max_date)
            
            print(f"Ingested {len(df)} new rows. New Watermark: {max_date}")
        else:
            print(f"No new data for {table_name}")
        
        conn.close()
        
    except Exception as e:
        print(f"Error ingesting {table_name}: {e}")

if __name__ == "__main__":
    tables_to_ingest = ['dim_users', 'dim_products', 'fact_sales']
    print("Starting Incremental Ingestion...")
    for table in tables_to_ingest:
        ingest_table(table)