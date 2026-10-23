{{ config(materialized='table') }}

SELECT DISTINCT
    customer_id,
    'Standard Tier' AS customer_segment
FROM {{ ref('stg_orders') }}
ORDER BY customer_id
