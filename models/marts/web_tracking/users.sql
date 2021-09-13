-- Description:
--   * This data model:
--   * Link users' browsing sessions together when we know that they are in fact the same user. 
--   * Create a session_id column for our pageviews table that groups together pageviews that are part of a continuous browsing session (per user).

-- High level logic:
--   * Gather all events and sort them by timestamp.
--   * Calculate lapse-time between last and current event.
--   * If laspe-time is greater than 30 minutes, the initiate new session count.
--   * Generate session_id with ["user_id"] and [sum of "session count"].

-- 1. User Stitching
with user_stitching as (
    select 
        pageview_id
        , visitor_id
        , customer_id        
        , case 
            when customer_id is not null then first_value(visitor_id) over (partition by customer_id order by timestamp) 
            else visitor_id
        end as user_id 
        , device_type
        , page_path
        , timestamp
    from {{ ref('stg_web_tracking__pageviews') }}
),

-- 2. Sessionization

-- 2.1 Use window function to find out the time difference between each pageview and the previous one for each user_id
time_between_pageviews as (
    select
        *
        , lag(timestamp) over (PARTITION BY user_id order by timestamp) -- returns the timestamp of the previous pageview (aka. row above) and null if no prior pageview
        , date_diff(timestamp, lag(timestamp) over (PARTITION BY user_id order by timestamp), minute) as inactivity_time -- returns the number of minutes between two pageviews
    from user_stitching
),

-- 2.2 Group pageviews event into sessions based on 30min intervals of inactivity

session_sequence as (
    select 
        user_id || '-' || row_number() over(partition by user_id order by timestamp) as session_id
        , user_id
        , timestamp as session_start_at
        , lead(timestamp) over(partition by user_id order by timestamp) as next_session_start_at
    from time_between_pageviews
    where inactivity_time > 30 or inactivity_time is null 
),

-- 2.3 Sessionization of each pageview event based on session_start and next_session_start timestamps 
sessionized as (
    select 
        session_sequence.session_id
        , session_sequence.session_start_at
        , session_sequence.next_session_start_at
        , us.user_id
        , us.id as pageview_id
        , us.timestamp
        , us.customer_id
        , us.visitor_id
        , us.device_type
        , us.page
    from session_sequence
    left join user_stitching us on us.user_id = session_sequence.user_id
    and us.timestamp >= session_sequence.session_start_at
    and (us.timestamp < session_sequence.next_session_start_at or session_sequence.next_session_start_at is null)
)

select 
    *
    {# count(*) as total_rows
    , count(distinct user_id) as total_user_id
    , count (distinct session_id) as total_session_id
    , count (distinct customer_id) as total_customer_id
    , count (distinct visitor_id) as total_visitor_id #}
from sessionized
