{% snapshot snap_customers %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='check',
        check_cols=[
            'master_customer_id', 'customer_segment', 'loyalty_tier',
            'account_status', 'city', 'country_code', 'preferred_payment_method'
        ]
    )
}}

select * from {{ ref('stg_customers') }}

{% endsnapshot %}
