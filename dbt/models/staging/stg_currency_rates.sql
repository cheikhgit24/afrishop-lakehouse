-- Taux vers la devise de reference (XOF). Valeurs neutres (1.0) par defaut :
-- voir la note du README sur la comparabilite des montants entre devises.
select
    currency,
    cast(rate_to_reference as numeric(12, 6)) as rate_to_reference
from {{ ref('currency_rates') }}
