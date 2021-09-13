select 
    visitor_id
    , customer_id
    , timestamp
    , case 
        when customer_id is not null then first_value(visitor_id) over (partition by customer_id order by timestamp) 
        else visitor_id
    end as user_id 
from `analytics-engineers-club.web_tracking.pageviews`