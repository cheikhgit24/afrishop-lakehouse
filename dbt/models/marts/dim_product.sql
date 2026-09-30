-- SCD type 2 : une ligne par version d'un produit (issue du snapshot snap_products).
with versions as (
    select
        *,
        row_number() over (partition by product_id order by dbt_valid_from) as version_no
    from {{ ref('snap_products') }}
)

select
    md5(product_id || '|' || dbt_valid_from::text) as product_key,
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
    is_perishable,
    product_status,
    dq_flag,
    case when version_no = 1 then timestamp '1900-01-01' else dbt_valid_from end as valid_from,
    coalesce(dbt_valid_to, timestamp '9999-12-31') as valid_to,
    dbt_valid_to is null as is_current
from versions
