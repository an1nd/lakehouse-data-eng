{{ config(materialized='ephemeral') }}

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    city,
    country,
    updated_at
FROM {{ ref('silver_customers') }}