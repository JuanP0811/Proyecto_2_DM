{{ config(materialized='table') }}

with bounds as (
    select
        min(date_trunc('day', pickup_ts))::date  as min_date,
        max(date_trunc('day', pickup_ts))::date  as max_date
    from silver.trips_silver_clean
    where pickup_ts is not null
),

dates as (
    select generate_series(min_date, max_date, interval '1 day')::date as date_day
    from bounds
)

select
    date_day,
    to_char(date_day, 'YYYY-MM-DD')                         as date_iso,

    extract(isodow from date_day)::int                      as day_of_week_iso,
    to_char(date_day, 'Day')                                as day_name,
    extract(day from date_day)::int                         as day_of_month,
    extract(doy from date_day)::int                         as day_of_year,

    extract(week from date_day)::int                        as week_of_year,
    extract(month from date_day)::int                       as month_num,
    to_char(date_day, 'Month')                              as month_name,
    extract(quarter from date_day)::int                     as quarter,
    extract(year from date_day)::int                        as year,

    (date_trunc('week', date_day::timestamp))::date         as week_start_date,
    (date_trunc('month', date_day::timestamp))::date        as month_start_date,
    (date_trunc('quarter', date_day::timestamp))::date      as quarter_start_date,
    (date_trunc('year', date_day::timestamp))::date         as year_start_date,

    case when extract(isodow from date_day) in (6, 7) then true else false end as is_weekend
from dates
order by date_day