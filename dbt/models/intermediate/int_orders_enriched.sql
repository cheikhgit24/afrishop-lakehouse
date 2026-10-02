-- Une ligne par commande : attributs de la commande + paiement retenu + livraison.
with selected_payment as (
    select
        order_id,
        payment_method
    from {{ ref('stg_payments') }}
    where is_selected
)

select
    o.order_id,
    o.customer_id,
    o.order_date,
    o.order_status,
    o.channel,
    o.country_code,
    o.currency,
    o.zone,
    o.is_partial_payment,
    coalesce(sp.payment_method, 'NONE') as payment_method,
    coalesce(d.delivery_status, 'NO_DELIVERY') as delivery_status,
    d.delivery_delay_days,
    coalesce(d.is_late, false) as is_late
from {{ ref('stg_orders') }} o
left join selected_payment sp on sp.order_id = o.order_id
left join {{ ref('stg_deliveries') }} d on d.order_id = o.order_id
