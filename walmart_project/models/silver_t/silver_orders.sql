{{ config(
    materialized='incremental',
    unique_key='order_id',
    tags=['silver_t']
) }}

SELECT
    order_id,
    customer_id,
    store_id,
    payment_method,
    LOWER(TRIM(order_status)) AS order_status,
    CAST(total_amount AS DECIMAL(10, 2)) AS total_amount,
    is_active,
    CAST(order_timestamp AS TIMESTAMP) AS order_timestamp,
    CAST(updated_timestamp AS TIMESTAMP) AS updated_at
FROM {{ source('walmart_bronze', 'orders') }}
WHERE total_amount > 0

{% if is_incremental() %}
  AND updated_timestamp >= (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}