{{ config(
    materialized='incremental',
    unique_key='customer_id',
    tags=['silver_t']
) }}

SELECT
    customer_id,
    TRIM(first_name) AS first_name,
    TRIM(last_name) AS last_name,
    LOWER(TRIM(email)) AS email,
    phone,
    city,
    province,
    country,
    is_active,
    CAST(updated_timestamp AS TIMESTAMP) AS updated_at
FROM {{ source('walmart_bronze', 'customers') }}

{% if is_incremental() %}
  WHERE updated_timestamp >= (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}