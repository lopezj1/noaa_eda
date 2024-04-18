{% set relation = source('raw', 'catch') %}
{% set null_proportion = 0.7 %}
{% set id_column = 'id_code' %}
{% set match_pattern = '[0-9]{16}' %}
{% set replace_pattern = '[^0-9]' %}

with drop_cols as (
    -- drop cols with high null proportion
    select
    {{ dbt_utils.star(from=relation, except=drop_cols_high_nulls(relation, null_proportion)) }}
    from {{ relation }}

)

select * from drop_cols

valid_records as (
    -- filter records where id_code is valid
    -- how to pass a cte as a relation to this macro look at the source code for deduplication macro
    {{ filter_id_code(relation, id_column, match_pattern, replace_pattern) }}

)

select * from valid_records