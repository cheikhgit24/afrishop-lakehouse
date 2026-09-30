-- Un produit ne peut avoir qu'une seule version courante.
select product_id
from {{ ref('dim_product') }}
where is_current
group by product_id
having count(*) > 1
