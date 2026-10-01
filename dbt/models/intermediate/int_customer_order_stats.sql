-- Statistiques de commande par client maitre (les comptes en doublon sont regroupes).
-- Seules les commandes "abouties" comptent : on exclut CANCELLED, RETURNED et PENDING.
-- Les montants sont convertis dans la devise de reference (taux neutres, voir README).
select
    c.master_customer_id,
    max(o.order_date) as last_order_at,
    min(o.order_date) as first_order_at,
    count(distinct o.order_id) as order_count,
    sum(o.total_amount * fx.rate_to_reference) as monetary_value
from {{ ref('stg_orders') }} o
join {{ ref('stg_customers') }} c on c.customer_id = o.customer_id
join {{ ref('stg_currency_rates') }} fx on fx.currency = o.currency
where o.order_status not in ('CANCELLED', 'RETURNED', 'PENDING')
group by c.master_customer_id
