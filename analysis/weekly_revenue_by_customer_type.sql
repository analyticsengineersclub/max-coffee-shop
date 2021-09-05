select 
    timestamp_trunc(created_at, week) as weekly
    , customer_type
    , sum(total_order_amount) as total_revenue
from `aec-cohort.dbt_max.customer_type`
group by 1,2
order by 1
