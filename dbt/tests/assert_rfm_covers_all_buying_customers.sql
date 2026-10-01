-- Tous les clients maitres ayant au moins une commande aboutie doivent etre dans le RFM.
select 1
where (select count(*) from {{ ref('mart_customer_rfm') }})
   <> (select count(*) from {{ ref('int_customer_order_stats') }})
