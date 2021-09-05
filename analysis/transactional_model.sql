select
    products.category,
    orders.state,
    
    sum(product_prices.price / 100) as total_sales

from `analytics-engineers-club.coffee_shop.order_items` as order_items

left join `analytics-engineers-club.coffee_shop.orders` as orders
    on order_items.order_id = orders.id

left join `analytics-engineers-club.coffee_shop.products` as products
    on order_items.product_id = products.id

left join `analytics-engineers-club.coffee_shop.product_prices` as product_prices
  on order_items.product_id = product_prices.product_id
  and orders.created_at between product_prices.created_at and product_prices.ended_at

where date_trunc(orders.created_at, year) = '2021-01-01'

group by 1, 2
order by 1, 2
limit 10
