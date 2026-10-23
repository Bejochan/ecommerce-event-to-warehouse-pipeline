{{ config(materialized='table') }}

SELECT
    s.order_id,
    s.customer_id,
    s.product_category,
    CAST(s.order_ts AS DATE) AS order_date,
    s.order_ts,
    s.payment_method,
    s.status,
    s.quantity,
    s.unit_price,
    s.discount_pct,
    s.net_sales
FROM {{ ref('stg_orders') }} s
