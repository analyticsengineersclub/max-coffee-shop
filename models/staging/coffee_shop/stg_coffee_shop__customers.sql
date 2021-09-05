with source as (
    select * from {{ source('coffee_shop', 'customers') }}
),

renamed as (
    select
        id as customer_id
        , name as first_last_name
        , email
    from source
)

select * from renamed 