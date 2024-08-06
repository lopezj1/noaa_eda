with
catches as (

        select * from {{ ref('stg_noaa__catches') }}

),

trips as (

        select * from {{ ref('int_trips_fips_join') }}

),

top_species as (

        select * from {{ ref('top_species') }}

),

joined as (

        select
        t.us_region,
        c.species_common_name,
        try_cast(t.caught as int) as caught
        from trips t
        left join catches c on c.survey_id = t.survey_id
        where 
        t.us_region is not null
        and
        t.caught is not null
        and
        c.species_common_name in (select species_common_name from top_species)

),

windowed as (

        select
        us_region,
        species_common_name,
        caught / sum(caught) over (partition by species_common_name) as catch_rate 
        from joined        

),

pivoted as (

        pivot windowed 
        on us_region
        using sum(catch_rate)
        order by species_common_name

)

select * from pivoted
