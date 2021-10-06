{% snapshot favorite_ice_cream_flavors %}

{{
    config(
      target_schema='dbt_snapshots',
      unique_key='id',
      strategy='timestamp',
      updated_at='updated_at',
    )
}}

select * from {{ ref('stg_google_form__favorite_ice_cream_flavors') }}

{% endsnapshot %}