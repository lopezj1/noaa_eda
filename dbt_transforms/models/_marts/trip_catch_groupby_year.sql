with trip_details as (

    select *
    from {{ ref('trip_details') }}

),

trip_catch_groupby_year as (
    
    select 
    trip_year, 
    count(*) as total_trips,
    cast(sum(total_number_fish_caught) as int) as total_fish
    from trip_details
    group by trip_year
    order by trip_year asc

)

select * from trip_catch_groupby_year