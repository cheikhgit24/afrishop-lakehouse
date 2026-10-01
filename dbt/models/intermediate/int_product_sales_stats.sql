-- Ventes agregees par produit (commandes abouties uniquement).
select
    l.product_id,
    sum(l.quantity) as units_sold,
    sum(l.line_total) as revenue,
    sum(l.line_margin) as margin,
    count(distinct l.order_id) as order_count,
    max(o.order_date) as last_sale_at,
    sum(case when o.order_date >= date '{{ var("as_of_date") }}' - 90 then l.quantity else 0 end)
        as units_last_90d
from {{ ref('int_order_lines_enriched') }} l
join {{ ref('int_orders_enriched') }} o on o.order_id = l.order_id
where o.order_status not in ('CANCELLED', 'RETURNED', 'PENDING')
group by l.product_id
