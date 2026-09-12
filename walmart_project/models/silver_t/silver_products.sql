{{ config(
    materialized='table',
    tags=['silver_t']
) }}

SELECT
    product_id,
    TRIM(product_name) AS product_name,
    TRIM(category) AS category,
    brand,
    CAST(price AS DECIMAL(10, 2)) AS price,
    is_active,
    CAST(updated_timestamp AS TIMESTAMP) AS updated_at
FROM {{ source('walmart_bronze', 'products') }}