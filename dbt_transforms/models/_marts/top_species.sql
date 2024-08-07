with 
catches as (

        select * from {{ ref('stg_noaa__catches') }}

),

top_species as (

        select 
        species_common_name, 
        count(species_common_name) as num_trips_where_species_targeted
        from catches
        group by species_common_name
        order by num_trips_where_species_targeted desc
        limit 10

)

select * from top_species