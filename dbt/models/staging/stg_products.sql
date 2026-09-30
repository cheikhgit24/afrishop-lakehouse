select
    product_id,
    sku,
    product_name,
    category_id,
    category_name,
    subcategory_name,
    brand,
    seller_id,
    unit_cost,
    unit_price,
    weight_grams,
    is_perishable,
    created_at,
    updated_at,
    product_status,
    dq_flag
from {{ source('curated', 'products') }}
