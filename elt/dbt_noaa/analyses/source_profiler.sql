{% set relation = source('raw', 'catch') %}
{% set null_proportion = 1.0 %}

select
{{ dbt_utils.star(from=relation, except=drop_cols_high_nulls(relation, null_proportion)) }}
from {{ relation }}
