with methods as (
    select distinct payment_method from {{ ref('stg_payments') }}
    union
    select 'NONE'
)

select
    md5(payment_method) as payment_method_key,
    payment_method,
    case
        when payment_method in ('OM_MOBILE_MONEY', 'WAVE', 'MTN_MOMO') then 'MOBILE_MONEY'
        when payment_method = 'CARD' then 'CARD'
        when payment_method = 'COD' then 'CASH_ON_DELIVERY'
        when payment_method = 'BANK_TRANSFER' then 'BANK'
        else 'NO_PAYMENT'
    end as payment_group
from methods
