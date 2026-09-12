{% snapshot customer_snapshot %}

{{
    config(
        target_schema='gold',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='updated_at'
    )
}}

SELECT * FROM {{ ref('dim_customers') }}

{% endsnapshot %}