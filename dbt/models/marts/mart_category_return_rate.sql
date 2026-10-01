-- Taux de retour par categorie, pays et mois. Modele incremental : les 2 derniers mois
-- sont recalcules a chaque execution (les statuts de commande evoluent apres coup).
{{
    config(
        materialized='incremental',
        unique_key='row_key',
        incremental_strategy='delete+insert'
    )
}}

with lines as (
    select
        to_char(o.order_date, 'YYYY-MM') as order_year_month,
        o.country_code,
        p.category_name,
        l.order_id,
        l.line_total,
        o.order_status
    from {{ ref('int_order_lines_enriched') }} l
    join {{ ref('int_orders_enriched') }} o on o.order_id = l.order_id
    join {{ ref('stg_products') }} p on p.product_id = l.product_id
    where o.order_status not in ('CANCELLED', 'PENDING')
    {% if is_incremental() %}
      and to_char(o.order_date, 'YYYY-MM') >=
          (select to_char(to_date(max(order_year_month), 'YYYY-MM') - interval '1 month', 'YYYY-MM')
           from {{ this }})
    {% endif %}
)

select
    md5(order_year_month || '|' || country_code || '|' || category_name) as row_key,
    order_year_month,
    country_code,
    category_name,
    count(distinct order_id) as orders_count,
    count(distinct order_id) filter (where order_status = 'RETURNED') as returned_orders_count,
    round(
        100.0 * count(distinct order_id) filter (where order_status = 'RETURNED')
        / count(distinct order_id), 2
    ) as return_rate_pct,
    sum(line_total) as gross_revenue,
    sum(line_total) filter (where order_status = 'RETURNED') as returned_revenue
from lines
group by order_year_month, country_code, category_name
