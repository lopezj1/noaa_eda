{% set fishing_seasons = ['Spring', 'Summer', 'Fall', 'Winter'] %}

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

total_trip_count as (

        select * from {{ ref('int_total_trip_count') }}

),

joined as (

        select
        t.trip_year,
        t.fishing_season,
        try_cast(t.caught as int) as caught,
        s.species_common_name,
        from trips t
        left join sizes s on s.fishing_trip_id = t.fishing_trip_id
        where t.fishing_season is not null
        and
        s.species_common_name in (

                                select species_common_name from top_species

        )

),

grouped as (

        select
        species_common_name,
        fishing_season,
        round(sum(caught) / (select * from total_trip_count) * 100, 2) as catch_rate
        from joined
        group by 1, 2
        order by 1, 2

),

pivoted as (

        pivot grouped 
        on fishing_season
        using avg(catch_rate)
        order by species_common_name

)

select * from pivoted
