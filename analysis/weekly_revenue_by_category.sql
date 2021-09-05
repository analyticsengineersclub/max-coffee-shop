select 
    timestamp_trunc(created_at, week) as weekly
    , category
    , sum(product_price) as total_revenue
from `aec-cohort.dbt_max.product_categories`
group by 1,2
order by 1
