-- Rotation des produits : volumes, classe ABC (CA cumule) et statut de rotation.
with stats as (
    select
        p.product_id,
        p.product_name,
        p.category_name,
        p.is_perishable,
        s.units_sold,
        s.revenue,
        s.margin,
        s.order_count,
        s.last_sale_at,
        s.units_last_90d,
        (date '{{ var("as_of_date") }}' - s.last_sale_at::date) as days_since_last_sale,
        sum(s.revenue) over (order by s.revenue desc, p.product_id)
        / sum(s.revenue) over () as cumulative_revenue_share
    from {{ ref('dim_product') }} p
    join {{ ref('int_product_sales_stats') }} s on s.product_id = p.product_id
    where p.is_current
)

select
    *,
    case
        when cumulative_revenue_share <= 0.80 then 'A'
        when cumulative_revenue_share <= 0.95 then 'B'
        else 'C'
    end as abc_class,
    case
        when days_since_last_sale > 90 then 'DORMANT'
        when units_last_90d < 3 then 'SLOW'
        else 'ACTIVE'
    end as rotation_status
from stats
