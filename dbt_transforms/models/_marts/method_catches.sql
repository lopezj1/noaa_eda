with
sizes as (

        select * from {{ ref('stg_noaa__sizes') }}

),

trips as (

        select * from {{ ref('int_trips_fips_join') }}

),

top_species as (

        select * from {{ ref('top_species') }}

),

joined as (

        select
        t.fishing_method_uncollapsed,
        s.species_common_name,
        try_cast(t.caught as int) as caught
        from trips t
        left join sizes s on s.survey_id = t.survey_id
        where 
        t.fishing_method_uncollapsed is not null
        and
        t.caught is not null
        and
        s.species_common_name in (select species_common_name from top_species)

),

windowed as (

        select
        fishing_method_uncollapsed,
        species_common_name,
        caught / sum(caught) over (partition by species_common_name) as catch_rate 
        from joined        

),

pivoted as (

        pivot windowed 
        on fishing_method_uncollapsed
        using sum(catch_rate)
        order by species_common_name

)

select * from pivoted
