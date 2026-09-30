-- Les paiements rattaches a une commande mise en quarantaine (455 lignes) sont exclus :
-- sans commande retenue, ils n'ont aucun sens dans l'entrepot analytique.
select
    payment_id,
    order_id,
    payment_method,
    payment_provider,
    payment_amount,
    currency,
    payment_status,
    payment_date,
    transaction_reference,
    is_selected
from {{ source('curated', 'payments') }}
where order_id in (select order_id from {{ source('curated', 'orders') }})
