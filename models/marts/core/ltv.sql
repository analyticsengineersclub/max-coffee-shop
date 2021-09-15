with product_categories as (
    select * from {{ ref('product_categories') }}
),

customers as (
    select * from {{ ref('dim_customers') }}
),

unnested_week_array as (
    select week from unnest(generate_date_array('2020-12-31', '2021-06-01', interval 1 week)) as week
),

cross_join as (
    select 
        w.week
        , c.customer_id
    from unnested_week_array w 
    cross join customers c
)

select * from cross_join