with trip_details as (

    select *
    from {{ ref('trip_details') }}

),

trip_catch_summary as (
    
    select 
    min(trip_year) as start_year,
    max(trip_year) as end_year,
    count(*) as total_trips,
    cast(sum(total_number_fish_caught) as int) as total_fish
    from trip_details

)

select * from trip_catch_summary