{# {% set  = ["coffee beans", "merch", "brewing supplies"] %} #}
{% set product_categories = dbt_utils.get_column_values(table=ref('fct_line_items'), column='category') %}

select
  date_trunc(created_at, month) as date_month,
  {% for product_category in  product_categories %}
    sum(case when category = '{{ product_category }}' then product_price end) as {{ product_category | replace(' ', '_') }}_amount  
    {% if not loop.last %} ,
    {% endif %}
  {% endfor %}
-- you may have to `ref` a different model here, depending on what you've built previously
from {{ ref('fct_line_items') }}
group by 1