{{ config(
    materialized='table',
    schema='gold',
    tags=['gold']
) }}

SELECT
    CAST(order_timestamp AS DATE) AS order_date,
    province,
    order_status,
    payment_method,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(total_amount) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS average_order_value,
    MAX(updated_at) AS last_updated_at
FROM {{ ref('obt_orders') }}
GROUP BY 1, 2, 3, 4