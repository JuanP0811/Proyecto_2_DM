{{ config(materialized='table') }}

with vendors as (

    select distinct
        vendorid::int as vendor_key
    from silver.trips_silver_clean
    where vendorid is not null

)

select
    vendor_key,
    case vendor_key
        when 1 then 'Creative Mobile Technologies'
        when 2 then 'VeriFone Inc.'
        else 'Other / Unknown'
    end as vendor_name
from vendors
order by vendor_key