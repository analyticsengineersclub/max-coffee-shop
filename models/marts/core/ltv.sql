{# Deprecated #}

with line_items as (
    select * from {{ ref('fct_line_items') }}
),

customers as (
    select * from {{ ref('dim_customers') }}
),

unnested_week_array as (
    select week from unnest(generate_date_array('2020-12-31', '2021-06-01', interval 1 week)) as week
),

customer_revenue as (
    select
        customer_id
        , date(date_trunc(created_at, week)) as order_week
        , sum(product_price) as weekly_revenue
    from line_items
    group by 1,2
),

cross_join as (
    select 
        w.week
        , c.customer_id
    from unnested_week_array w 
    cross join customers c
),

weekly_customer_revenue as (
    select
        cj.week
        , cr.customer_id
        , cr.weekly_revenue
    from cross_join cj
    left join customer_revenue cr on 
        cj.week = cr.order_week
        and cj.customer_id = cr.customer_id
)

select * from weekly_customer_revenue