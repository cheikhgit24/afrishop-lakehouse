select
    source,
    record_key,
    reason,
    cast(run_date as date) as run_date,
    payload
from {{ source('curated', 'quarantine') }}
