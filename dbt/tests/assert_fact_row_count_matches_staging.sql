-- La jointure avec les dimensions ne doit ni perdre ni dupliquer de lignes.
select 1
where
    (select count(*) from {{ ref('fact_order_line') }})
    <> (select count(*) from {{ ref('stg_order_lines') }})
