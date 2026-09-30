select
    customer_id,
    master_customer_id,
    is_duplicate_account,
    email_hash,
    phone_hash,
    birth_year,
    gender,
    registration_date,
    country_code,
    city,
    customer_segment,
    preferred_payment_method,
    loyalty_tier,
    account_status
from {{ source('curated', 'customers') }}
