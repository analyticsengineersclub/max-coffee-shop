with 

orders as (
    select * from {{ ref('stg_coffee_shop__orders') }}
),

customers as (
    select * from {{ ref('stg_coffee_shop__customers') }}
),

final as (
    select 
        orders.order_id
        , customers.customer_id
        , case when RANK() OVER (PARTITION BY customers.customer_id ORDER BY orders.created_at) = 1 then 'new' else 'returning' end as customer_type
        , orders.created_at
        , orders.total_order_amount
    from orders

    left join customers
        on orders.customer_id = customers.customer_id
)

select * from final

