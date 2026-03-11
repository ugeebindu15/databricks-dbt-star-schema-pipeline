{{ config(materialized='table', schema='golds') }}

with cte as (
    select
        p.name,
        p.gender,
        f.amount,
        f.booking_date
    from {{ source('golds', 'fact_bookings') }} f
    left join {{ source('golds', 'dim_passengers') }} p
        on f.Dimpassengerkey = p.Dimpassengerkey
)

select
    name,
    gender,
    count(*) as total_bookings,
    sum(amount) as total_spent
from cte
group by name, gender
