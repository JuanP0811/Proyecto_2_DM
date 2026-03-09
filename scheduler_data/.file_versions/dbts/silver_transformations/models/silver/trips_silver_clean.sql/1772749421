{{ config(materialized='view') }}

select *
from silver.trips_silver
where
    pickup_ts is not null
    and pickup_ts >= timestamp '2022-01-01'
    and pickup_ts <  timestamp '2026-01-01'