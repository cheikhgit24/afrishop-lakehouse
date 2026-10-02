-- Grain : une ligne de commande. Les dimensions SCD2 sont jointes "a la date de la commande".
select
    l.order_line_id,
    l.order_id,
    coalesce(c.customer_key, '-1') as customer_key,
    p.product_key,
    to_char(o.order_date, 'YYYYMMDD')::int as order_date_key,
    pm.payment_method_key,
    ds.delivery_status_key,
    o.country_code,
    o.currency,
    o.channel,
    o.order_status,
    o.is_partial_payment,
    o.is_late,
    l.seller_id,
    l.quantity,
    l.unit_price,
    l.line_discount,
    l.line_total,
    l.line_cost,
    l.line_margin
from {{ ref('int_order_lines_enriched') }} l
join {{ ref('int_orders_enriched') }} o on o.order_id = l.order_id
left join {{ ref('dim_customer') }} c
    on
        c.customer_id = o.customer_id
        and o.order_date >= c.valid_from
        and o.order_date < c.valid_to
join {{ ref('dim_product') }} p
    on
        p.product_id = l.product_id
        and o.order_date >= p.valid_from
        and o.order_date < p.valid_to
join {{ ref('dim_payment_method') }} pm on pm.payment_method = o.payment_method
join {{ ref('dim_delivery_status') }} ds on ds.delivery_status = o.delivery_status
