-- Un client ne peut avoir qu'une seule version courante.
select customer_id
from {{ ref('dim_customer') }}
where is_current
group by customer_id
having count(*) > 1
