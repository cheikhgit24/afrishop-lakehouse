-- Segmentation RFM (Recence, Frequence, Montant) par client maitre.
-- Scores de 1 a 5 par quintiles ; 5 = meilleur. Date de reference : variable as_of_date.
with base as (
    select
        master_customer_id,
        last_order_at,
        order_count,
        monetary_value,
        (date '{{ var("as_of_date") }}' - last_order_at::date) as recency_days
    from {{ ref('int_customer_order_stats') }}
),

scored as (
    select
        *,
        6 - ntile(5) over (order by recency_days asc) as r_score,
        ntile(5) over (order by order_count asc, monetary_value asc) as f_score,
        ntile(5) over (order by monetary_value asc) as m_score
    from base
)

select
    master_customer_id,
    last_order_at,
    recency_days,
    order_count,
    round(monetary_value::numeric, 2) as monetary_value,
    r_score,
    f_score,
    m_score,
    r_score::text || f_score::text || m_score::text as rfm_code,
    case
        when r_score >= 4 and f_score >= 4 and m_score >= 4 then 'CHAMPIONS'
        when r_score >= 3 and f_score >= 3 then 'LOYAL'
        when r_score >= 4 and f_score <= 2 then 'NEW_OR_PROMISING'
        when r_score <= 2 and f_score >= 3 then 'AT_RISK'
        when r_score <= 2 and f_score <= 2 and m_score <= 2 then 'LOST'
        else 'NEED_ATTENTION'
    end as rfm_segment
from scored
