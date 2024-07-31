{% set src = source('raw', 'catch') %}

with merge_cols as (

    {{ merge_duplicate_column_names(src) }}

)

select * from merge_cols