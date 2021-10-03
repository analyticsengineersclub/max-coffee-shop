with source as (

    select * from {{ source('advanced_dbt_examples', 'form_events') }}

),

renamed as (

    select
        timestamp,
        github_username,
        event

    from source

)

select * from renamed