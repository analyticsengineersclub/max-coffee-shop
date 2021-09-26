{% set product_categories = ["coffee beans", "merch", "brewing supplies"] %}

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