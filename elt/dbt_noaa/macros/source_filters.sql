{% macro drop_cols_high_nulls(source_table, null_threshold=0.90) %}

{% set query %}
with profile as ({{ dbt_profiler.get_profile(relation=source_table) }})

select column_name from profile 
where not_null_proportion < {{ null_threshold }}
{% endset %}

{% set results = run_query(query) %}

{% if execute %}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}   
{% endif %}

{{ return(results_list) }}

{% endmacro %}