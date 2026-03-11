

{{ config(
    materialized='table',
    schema='golds'
) }}

with cte as (
    select
        f.amount,
        d.country
    from {{ source('golds', 'fact_bookings') }} f
    left join {{ source('golds', 'dim_airports') }} d
        on f.Dimairportkey = d.Dimairportkey
)

select
    country,
    sum(amount) as total_amount
from cte
group by country