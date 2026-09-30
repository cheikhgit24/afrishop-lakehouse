with days as (
    select generate_series(date '2024-01-01', date '2027-12-31', interval '1 day')::date as date_day
)

select
    to_char(date_day, 'YYYYMMDD')::int as date_key,
    date_day as date,
    extract(year from date_day)::int as year,
    extract(quarter from date_day)::int as quarter,
    extract(month from date_day)::int as month,
    to_char(date_day, 'YYYY-MM') as year_month,
    extract(day from date_day)::int as day,
    extract(isodow from date_day)::int as day_of_week,
    extract(isodow from date_day) in (6, 7) as is_weekend
from days
