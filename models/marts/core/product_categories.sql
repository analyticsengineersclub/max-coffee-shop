with 

orders as (
    select * from {{ ref('stg_coffee_shop__orders') }}
),

order_items as (
    select * from {{ ref('stg_coffee_shop__order_items') }}
),

products as (
    select * from {{ ref('stg_coffee_shop__products') }}
),

product_prices as (
    select * from {{ ref('stg_coffee_shop__product_prices') }}
),

final as (
    select 
        order_items.order_item_id
        , orders.created_at
        , products.category
        , product_prices.product_price

    from order_items

    left join orders
        on order_items.order_id = orders.order_id

    left join products
        on order_items.product_id = products.product_id

    left join product_prices
        on order_items.product_id = product_prices.product_id
        and orders.created_at between product_prices.created_at and product_prices.ended_at
)

select * from final