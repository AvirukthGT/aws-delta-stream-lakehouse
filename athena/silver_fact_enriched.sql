CREATE TABLE ecommerce_silver_db.fact_sales_enriched
WITH (
      format = 'PARQUET',
      external_location = 's3://event-driven-lakehouse-clean-b8ad22ee/silver/fact_sales_enriched/',
      write_compression = 'SNAPPY'
) AS
WITH 
latest_users AS (
    SELECT * FROM (
        SELECT *, 
               ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY updated_at DESC) as rn
        FROM "ecommerce_raw_db"."dim_users"
    ) WHERE rn = 1
),
latest_sales AS (
    SELECT * FROM (
        SELECT *, 
               ROW_NUMBER() OVER (PARTITION BY sale_id ORDER BY updated_at DESC) as rn
        FROM "ecommerce_raw_db"."fact_sales"
    ) WHERE rn = 1
)
SELECT 
    s.sale_id,
    s.quantity,
    s.total_amount,
    s.status,
    s.order_date,
    u.user_id,
    u.name as user_name,
    u.email,
    u.city,
    p.product_name,
    p.category,
    p.price as unit_price
FROM latest_sales s
JOIN latest_users u ON s.user_id = u.user_id
JOIN "ecommerce_raw_db"."dim_products" p ON s.product_id = p.product_id;