-- Le chiffre d'affaires des lignes doit etre identique entre le staging et la table de faits.
select 1
where abs(
    (select sum(line_total) from {{ ref('fact_order_line') }})
  - (select sum(line_total) from {{ ref('stg_order_lines') }})
) > 0.01
