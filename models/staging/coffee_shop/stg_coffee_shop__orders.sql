with source as (
    select * from {{ source('coffee_shop', 'orders') }}
),

renamed as (
    select
        id as order_id
        , customer_id
        , created_at
        , total as total_order_amount
        , address as order_address
        , state as order_state
        , zip as order_zip
    from source
)

select * from renamed 