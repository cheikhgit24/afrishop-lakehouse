-- Une ligne par ligne de commande, avec cout et marge (cout unitaire courant du catalogue).
select
    ol.order_line_id,
    ol.order_id,
    ol.product_id,
    ol.seller_id,
    ol.quantity,
    ol.unit_price,
    ol.line_discount,
    ol.line_total,
    p.unit_cost,
    ol.quantity * p.unit_cost as line_cost,
    ol.line_total - ol.quantity * p.unit_cost as line_margin
from {{ ref('stg_order_lines') }} ol
join {{ ref('stg_products') }} p on p.product_id = ol.product_id
