{{ config(materialized='table') }}

select
    payment_type::int as payment_type_key,
    case payment_type::int
        when 1 then 'Credit card'
        when 2 then 'Cash'
        when 3 then 'No charge'
        when 4 then 'Dispute'
        when 5 then 'Unknown'
        when 6 then 'Voided trip'
        else 'Other'
    end as payment_type_name
from silver.trips_silver_clean
where payment_type is not null
group by payment_type
order by payment_type_key