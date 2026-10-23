{{ config(materialized='view') }}

WITH raw_source AS (
    SELECT * FROM read_parquet('../data/valid/orders_valid.parquet')
)

SELECT
    order_id::VARCHAR AS order_id,
    customer_id::VARCHAR AS customer_id,
    strptime(replace(order_ts, 'Z', ''), '%Y-%m-%dT%H:%M:%S')::TIMESTAMP AS order_ts,
    product_category::VARCHAR AS product_category,
    quantity::INTEGER AS quantity,
    unit_price::DOUBLE AS unit_price,
    discount_pct::DOUBLE AS discount_pct,
    payment_method::VARCHAR AS payment_method,
    status::VARCHAR AS status,
    (quantity * unit_price * (1.0 - discount_pct))::DOUBLE AS net_sales
FROM raw_source
