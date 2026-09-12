{{ config(
    materialized='table',
    tags=['silver_b']
) }}

{% set joins = [
    {
        'table': ref('silver_customers'),
        'alias': 'c',
        'on': 'o.customer_id = c.customer_id'
    }
] %}

SELECT
    o.order_id,
    o.customer_id,
    o.store_id,
    o.payment_method,
    o.order_status,
    o.total_amount,
    c.first_name,
    c.last_name,
    c.email,
    c.city,
    c.province,
    c.country,
    o.order_timestamp,
    o.updated_at
FROM {{ ref('silver_orders') }} o
{% for j in joins %}
LEFT JOIN {{ j.table }} {{ j.alias }}
    ON {{ j.on }}
{% endfor %}