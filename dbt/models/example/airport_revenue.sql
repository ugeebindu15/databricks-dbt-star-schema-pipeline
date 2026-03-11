{{ config(materialized='table', schema='golds') }}

with cte as (
    select
        a.country,
        a.airport_id,
        f.amount,
        f.booking_date
    from {{ source('golds', 'fact_bookings') }} f
    left join {{ source('golds', 'dim_airports') }} a
        on f.Dimairportkey = a.Dimairportkey
)

select
    country,
    airport_id,
    count(*) as total_bookings,
    sum(amount) as total_revenue
from cte
group by country, airport_id
