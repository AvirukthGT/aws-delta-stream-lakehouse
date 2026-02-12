# src/generator/check_db.py
import os
import psycopg2
from dotenv import load_dotenv
from tabulate import tabulate # Run 'pip install tabulate' for pretty output

load_dotenv()

def check_data():
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            port=os.getenv("DB_PORT")
        )
        cur = conn.cursor()
        
        # Check the Fact Table
        cur.execute("SELECT * FROM fact_sales LIMIT 5;")
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        
        print("\n--- Latest 5 Sales in RDS ---")
        print(tabulate(rows, headers=colnames, tablefmt="grid"))
        
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Connection failed: {e}")

if __name__ == "__main__":
    check_data()