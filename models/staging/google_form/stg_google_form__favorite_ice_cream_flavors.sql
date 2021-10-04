with source as (

    select * from {{ source('advanced_dbt_examples', 'favorite_ice_cream_flavors') }}

),

renamed as (

    select
        updated_at,
        first_name,
        favorite_ice_cream_flavor,
        id

    from source

)

select * from renamed