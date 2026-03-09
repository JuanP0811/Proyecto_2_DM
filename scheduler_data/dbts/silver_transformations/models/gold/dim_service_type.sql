{{ config(materialized='table') }}

select distinct
    service_type as service_type_key,
    service_type as service_type_name
from silver.trips_silver_clean
where service_type is not null
order by service_type