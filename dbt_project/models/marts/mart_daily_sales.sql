{{ config(materialized='table') }}

SELECT
    order_date,
    product_category,
    payment_method,
    COUNT(order_id) AS total_orders,
    SUM(quantity) AS total_quantity,
    ROUND(SUM(net_sales), 2) AS net_sales
FROM {{ ref('fct_orders') }}
GROUP BY order_date, product_category, payment_method
ORDER BY order_date, product_category
