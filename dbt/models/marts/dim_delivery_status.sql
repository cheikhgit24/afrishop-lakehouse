with statuses as (
    select distinct delivery_status from {{ ref('stg_deliveries') }}
    union
    select 'NO_DELIVERY'
)

select
    md5(delivery_status) as delivery_status_key,
    delivery_status,
    delivery_status in ('DELIVERED', 'FAILED', 'RETURNED_TO_WAREHOUSE') as is_final,
    delivery_status = 'DELIVERED' as is_successful
from statuses
