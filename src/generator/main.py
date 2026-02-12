import psycopg2
from faker import Faker
import time
import random
import os
from dotenv import load_dotenv

load_dotenv()
fake = Faker()

def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME", "ecommerce_db"),
        user=os.getenv("DB_USER", "dbadmin"),
        password=os.getenv("DB_PASS"),
        port=os.getenv("DB_PORT", 5432)
    )

def setup_star_schema(conn):
    with conn.cursor() as cur:
        # 1. Dim_Users
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_users (
                user_id SERIAL PRIMARY KEY,
                name TEXT,
                email TEXT UNIQUE,
                city TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        # 2. Dim_Products
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_products (
                product_id SERIAL PRIMARY KEY,
                product_name TEXT,
                category TEXT,
                price DECIMAL(10,2),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        # 3. Fact_Sales (The Transactional Center)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fact_sales (
                sale_id SERIAL PRIMARY KEY,
                user_id INT REFERENCES dim_users(user_id),
                product_id INT REFERENCES dim_products(product_id),
                quantity INT,
                total_amount DECIMAL(10,2),
                status TEXT,
                order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Pre-populate Dim_Products if empty
        cur.execute("SELECT count(*) FROM dim_products")
        if cur.fetchone()[0] == 0:
            products = [
                ('Laptop', 'Electronics', 1200.00), ('Smartphone', 'Electronics', 800.00),
                ('Desk Chair', 'Furniture', 250.00), ('Coffee Maker', 'Appliances', 80.00),
                ('Running Shoes', 'Apparel', 120.00)
            ]
            cur.executemany("INSERT INTO dim_products (product_name, category, price) VALUES (%s, %s, %s)", products)
        
        conn.commit()
    print("Star Schema Initialized (Users, Products, Sales).")

def generate_data():
    conn = get_conn()
    setup_star_schema(conn)
    statuses = ['PENDING', 'SHIPPED', 'DELIVERED', 'CANCELLED']

    print("Starting Star Schema data generation...")
    while True:
        try:
            with conn.cursor() as cur:
                # 1. Randomly add new users
                if random.random() > 0.8:
                    cur.execute(
                        "INSERT INTO dim_users (name, email, city) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                        (fake.name(), fake.email(), fake.city())
                    )

                # Get random user and product IDs for the fact table
                cur.execute("SELECT user_id FROM dim_users ORDER BY RANDOM() LIMIT 1")
                res = cur.fetchone()
                if not res: continue # Skip if no users yet
                user_id = res[0]

                cur.execute("SELECT product_id, price FROM dim_products ORDER BY RANDOM() LIMIT 1")
                product_id, price = cur.fetchone()

                # 2. Insert into Fact_Sales
                qty = random.randint(1, 3)
                cur.execute(
                    "INSERT INTO fact_sales (user_id, product_id, quantity, total_amount, status) VALUES (%s, %s, %s, %s, %s)",
                    (user_id, product_id, qty, qty * price, 'PENDING')
                )

                # 3. CDC: Update a random existing sale
                cur.execute("""
                    UPDATE fact_sales 
                    SET status = %s, updated_at = CURRENT_TIMESTAMP 
                    WHERE sale_id = (SELECT sale_id FROM fact_sales ORDER BY RANDOM() LIMIT 1)
                """, (random.choice(statuses),))

                conn.commit()
                print(f"Sale Recorded: User {user_id} bought Product {product_id}")
            time.sleep(3)
        except Exception as e:
            print(f"Error: {e}")
            conn.rollback()
            time.sleep(5)

if __name__ == "__main__":
    generate_data()