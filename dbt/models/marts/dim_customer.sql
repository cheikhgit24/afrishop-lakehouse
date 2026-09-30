-- SCD type 2 : une ligne par version d'un client (issue du snapshot snap_customers).
-- La premiere version est valide depuis 1900 pour que la jointure "a la date de la
-- commande" fonctionne aussi pour les commandes anterieures au premier snapshot.
with versions as (
    select
        *,
        row_number() over (partition by customer_id order by dbt_valid_from) as version_no
    from {{ ref('snap_customers') }}
)

select
    md5(customer_id || '|' || dbt_valid_from::text) as customer_key,
    customer_id,
    master_customer_id,
    is_duplicate_account,
    gender,
    birth_year,
    registration_date,
    country_code,
    city,
    customer_segment,
    preferred_payment_method,
    loyalty_tier,
    account_status,
    case when version_no = 1 then timestamp '1900-01-01' else dbt_valid_from end as valid_from,
    coalesce(dbt_valid_to, timestamp '9999-12-31') as valid_to,
    dbt_valid_to is null as is_current
from versions

union all

select
    '-1', 'UNKNOWN', 'UNKNOWN', false, null, null, null, null, null,
    'UNKNOWN', null, null, null,
    timestamp '1900-01-01', timestamp '9999-12-31', true
