with catches as (

        select * from {{ ref('stg_noaa__catches') }}

),

sizes as (

        select * from {{ ref('stg_noaa__sizes') }}

),

catches_sizes_joined as (

        select
        *
        from catches c
        left join sizes s on s.survey_species_id = c.survey_species_id

)

select * from catches_sizes_joined