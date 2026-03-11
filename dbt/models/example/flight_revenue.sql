{{ config(materialized='table', schema='golds') }}

with cte as (
    select
        fl.airline,
        fl.origin,
        fl.destination,
        f.amount,
        f.booking_date
    from {{ source('golds', 'fact_bookings') }} f
    left join {{ source('golds', 'dim_flights') }} fl
        on f.Dimflightkey = fl.Dimflightkey
)

select
    airline,
    origin,
    destination,
    count(*) as total_bookings,
    sum(amount) as total_revenue
from cte
group by airline, origin, destination
