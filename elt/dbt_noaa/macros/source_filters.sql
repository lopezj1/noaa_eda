{% macro drop_cols_high_nulls(relation, null_proportion=0.70) %}

{% set query %}
with profile as ({{ dbt_profiler.get_profile(relation=relation) }})

select column_name from profile 
where not_null_proportion < {{ null_proportion }}
{% endset %}

{% set results = run_query(query) %}

{% if execute %}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}   
{% endif %}

{{ return(results_list) }}

{% endmacro %}

{% macro filter_id_code(relation, id_column, match_pattern, replace_pattern) %}

with unfiltered as (

    select * from {{ relation }}
),

filtered as (

    select * from unfiltered
    where regexp_matches({{ id_column }}, '{{ match_pattern }}')

),

fixed as (

    select * replace(regexp_replace({{ id_column }}, '{{ replace_pattern }}', '') as {{ id_column }}) from filtered

)

select * from fixed

{% endmacro %}
