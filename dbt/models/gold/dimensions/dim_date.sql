with bounds as (
    select
        coalesce(min(order_date)::date, current_date - 365) as first_day,
        greatest(coalesce(max(order_date)::date, current_date), current_date) as last_day
    from {{ source('silver', 'orders') }}
)

select
    day::date                                   as date_day,
    extract(year from day)::int                 as year,
    extract(quarter from day)::int              as quarter,
    extract(month from day)::int                as month,
    to_char(day, 'YYYY-MM')                     as year_month,
    extract(isodow from day)::int               as iso_day_of_week,
    to_char(day, 'Dy')                          as day_name,
    extract(isodow from day) in (6, 7)          as is_weekend,
    date_trunc('week', day)::date               as week_start
from bounds,
     generate_series(bounds.first_day, bounds.last_day, interval '1 day') as day
