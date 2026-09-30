select
    delivery_id,
    order_id,
    carrier,
    tracking_number,
    delivery_status,
    shipped_at,
    delivered_at,
    delivery_attempts,
    delivery_cost,
    delivery_delay_days,
    is_late,
    status_conflict
from {{ source('curated', 'deliveries') }}
