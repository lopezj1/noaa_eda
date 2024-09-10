with trip_details as (

    select *
    from {{ ref('trip_details') }}

),

top_species_region as (

    select 
    us_region,
    species_common_name,
    sum(total_number_fish_caught) as total_fish
    from trip_details
    group by us_region, species_common_name
    order by us_region, total_fish desc

),

ranked_species as (

    select 
    us_region,
    species_common_name,
    total_fish,
    row_number() over (partition by us_region order by total_fish desc) as species_rank
    from top_species_region

),

catch_by_category_filtered as (
    
    select 
    td.us_region,    
    fishing_season, 
    fishing_method_uncollapsed,
    case 
        when species_rank <= 10 then td.species_common_name
        else 'Other'
    end as species_common_name,
    cast(total_number_fish_caught as int) as total_fish
    from trip_details td
    join ranked_species rs on td.us_region = rs.us_region and td.species_common_name = rs.species_common_name

)

select * from catch_by_category_filtered