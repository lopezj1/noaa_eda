with trip_details as (

    select * from {{ ref('trip_details') }}

),

fips_catches as (

    select 
    fips_code_where_caught, 
    cast(sum(total_number_fish_caught) as int) as total_fish
    from trip_details
    group by fips_code_where_caught

)

select * from fips_catches