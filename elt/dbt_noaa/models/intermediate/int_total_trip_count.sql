with 
trips as (

        select * from {{ ref('stg_noaa__trips') }}

),

total_trip_count as (

        select 
        count(try_cast(caught as int))
        from trips
        where fishing_season is not null

)

select * from total_trip_count