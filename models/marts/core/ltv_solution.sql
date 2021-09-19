{# Solution from Claire: https://github.com/analyticsengineersclub/sample-dbt-project/blob/customer-ltv/models/customers_week_generated.sql #}

with week_generated as (
    select * from {{ ref('week_generated') }}
),

orders as (
    select * from {{ ref('orders') }}
),

customer_weekly_orders as (
    select
        customer_id,
        date_trunc(sold_at, week) as date_week,
        sum(total_order_amount) as total_revenue
    from orders
    group by 1, 2
),

-- find the first week so we can only look at weeks onwards for each
customer_first_week as (
    select
        customer_id,
        min(date_week) as first_date_week
    from customer_weekly_orders
    group by 1
),

-- create one record per week since a customer first spent money
spined as (
    select
        customer_first_week.customer_id,
        week_generated.date_week,

        -- needed to do some casting to make this function work ¯\_(ツ)_/¯
        date_diff(
            cast(week_generated.date_week as datetime),
            cast(customer_first_week.first_date_week as datetime),
            week
        ) as week_number
    from week_generated

    inner join customer_first_week
        on week_generated.date_week >= customer_first_week.first_date_week
),

-- join back on the weekly spend, calc cumulative spend
joined as (
    select
        spined.customer_id,
        spined.week_number,
        spined.date_week,

        coalesce(customer_weekly_orders.total_revenue, 0) as weekly_revenue,

        sum(coalesce(customer_weekly_orders.total_revenue, 0)) over (
            partition by spined.customer_id
            order by spined.week_number
            rows between unbounded preceding and current row
        ) as cumulative_revenue

    from spined

    left join customer_weekly_orders
        on spined.customer_id = customer_weekly_orders.customer_id
        and spined.date_week = customer_weekly_orders.date_week
)

select * from joined