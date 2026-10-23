{{ config(materialized='table') }}

SELECT DISTINCT
    product_category,
    CASE 
        WHEN product_category = 'Electronics' THEN 'High-Value'
        WHEN product_category = 'Books' THEN 'Media'
        ELSE 'General Merchandise'
    END AS department
FROM {{ ref('stg_orders') }}
ORDER BY product_category
