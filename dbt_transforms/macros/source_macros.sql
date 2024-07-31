{% macro drop_cols_high_nulls(relation, null_proportion=0.75) %}

    {{ log(relation, info=True) }}
    {{ log(null_proportion, info=True) }}

    {% set query %}
    with profile as (
        
        {{ dbt_profiler.get_profile(relation=relation) }}
        
    )

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

{% macro merge_duplicate_column_names(relation) %}

    -- Returns a list of the columns from a relation, so you can then iterate in a for loop
    {%- set all_column_names = dbt_utils.get_filtered_columns_in_relation(from=relation) -%}
    {%- set duplicate_column_names = [] %}
    {%- set original_column_names = [] %}

    --{{ all_column_names }}

    -- Loop through column names and append any duplicated column names to a new list
    {% for all_column_name in all_column_names -%}
        {%- if all_column_name[-2] == '_' and all_column_name[-1] in ['1','2','3'] -%}
            {%- set _ = duplicate_column_names.append(all_column_name.lower()) -%}
        {%- endif -%}
    {%- endfor %}

    --{{ duplicate_column_names }}

    -- Get the original column name and append to separate list
    {% for duplicate_column_name in duplicate_column_names %}
        {%- set _ = original_column_names.append(duplicate_column_name[:-2]) -%}
    {% endfor %}

    --{{ original_column_names }}

    -- Merge the original and duplicated column name using coalesce
    select 
    {% for duplicate_column_name in duplicate_column_names -%}
        coalesce({{ duplicate_column_name[:-2] }}, {{ duplicate_column_name }}) as {{ duplicate_column_name[:-2] }},
    {% endfor -%}
    {% for all_column_name in all_column_names -%}
        {% if all_column_name not in original_column_names and all_column_name not in duplicate_column_names %}
    {{ all_column_name }}
            {%- if not loop.last -%}
                ,
            {%- endif -%}
        {% endif -%}
    {% endfor %}
    from {{ relation }}

{% endmacro %}
