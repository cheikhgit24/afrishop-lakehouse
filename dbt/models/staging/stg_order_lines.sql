select
    order_line_id,
    order_id,
    product_id,
    product_sku,
    quantity,
    unit_price,
    line_discount,
    line_total,
    seller_id,
    order_year_month
from {{ source('curated', 'order_lines') }}
