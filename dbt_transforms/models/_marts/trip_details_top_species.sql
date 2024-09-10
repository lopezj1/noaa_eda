with trip_details as (

    select *
    from {{ ref('trip_details') }}

),

top_species as (

    select *
    from {{ ref('top_species') }}

),

trip_details_top_species as (
    
    select
    trip_date,
    trip_year,
    species_common_name,
    us_region,
    fishing_season,
    fishing_method_uncollapsed,
    total_length_fish_harvested_mm,
    total_weight_fish_harvested_kg, 
    trip_fishing_effort_hours,
    number_of_outings_in_last_2_months,
    number_of_outings_in_last_year,
    total_number_fish_caught
    from trip_details 
    where species_common_name in (select species_common_name from top_species)

)

select * from trip_details_top_species