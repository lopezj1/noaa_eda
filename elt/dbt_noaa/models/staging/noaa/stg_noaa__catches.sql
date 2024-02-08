with source as (

    select * from {{ source('raw', 'catch') }}

),

renamed as (

    select
        cast(id_code as bigint) as fishing_trip_id,
        cast(strptime(date_published, '%m/%d/%Y') as date) as data_publish_date,
        cast(substring(id_code, 6, 4) as int) as trip_year,
        cast(substring(id_code, 10, 2) as int) trip_month_num,
        cast(substring(id_code, 12, 2) as int) as trip_day_num,
        case
            when 
            trip_month_num in (1,3,5,7,8,10,12) and trip_day_num <= 31
            or
            trip_month_num in (4,6,9,11) and trip_day_num <= 30
            or
            trip_month_num = 2 and trip_day_num <= 29
            then make_date(trip_year, trip_month_num, trip_day_num) 
            else NULL
        end as trip_date,
        dayname(trip_date) as trip_day_of_week,
        monthname(trip_date) as trip_month_name,
        case
            when trip_month_name in ('January', 'February') then 'Winter'
            when trip_month_name in ('March', 'April', 'May', 'June') then 'Spring'
            when trip_month_name in ('July', 'August') then 'Summer'
            when trip_month_name in ('September', 'October', 'November', 'December') then 'Fall'
            else NULL
        end as fishing_season,
        case
            when wave = 1 then 'January/February'
            when wave = 2 then 'March/April'
            when wave = 3 then 'May/June'
            when wave = 4 then 'July/August'
            when wave = 5 then 'September/October'
            when wave = 6 then 'November/December'
            else NULL
        end as sampling_period,
        case 
            when kod = 'wd' then false
            when kod = 'we' then true
            else NULL
        end as weekend,
        case 
            when sub_reg = 4 then 'North Atlantic (ME; NH; MA; RI; CT)'
            when sub_reg = 5 then 'Mid-Atlantic (NY; NJ; DE; MD; VA) '
            when sub_reg = 6 then 'South Atlantic (NC; SC; GA; EFL)'
            when sub_reg = 7 then 'Gulf of Mexico (WFL; AL; MS; LA)'
            when sub_reg = 8 then 'West Pacific (HI)'
            when sub_reg = 11 then 'U.S. Caribbean (Puerto Rico and Virgin Islands)'
            else NULL
        end as us_region,
        case
            when area_x = 1 then 'Ocean - Within 3 miles'
            when area_x = 2 then 'Ocean - Outside 3 miles'
            when area_x = 3 then 'Ocean - Within 10 miles'
            when area_x = 4 then 'Ocean - Outside 10 miles'
            when area_x = 5 then 'Inland'
            else NULL
        end as nautical_zone,
        case 
            when mode_fx = 1 then 'Man-Made'
            when mode_fx = 2 then 'Beach/Bank'
            when mode_fx = 3 then 'Shore'
            when mode_fx = 4 then 'Headboat'
            when mode_fx = 5 then 'Charter Boat'
            when mode_fx = 6 then 'Charter Boat'
            when mode_fx = 7 then 'Private/Rental Boat'
            else NULL
        end as fishing_method_collapsed,
        cast(st as int) as state_code_where_caught,
        cast(ceiling(cast(claim as float)) as int) as num_fish_harvested_observed_adjusted,
        cast(ceiling(cast(claim_unadj as float)) as int) as num_fish_harvested_observed_unadjusted,
        cast(ceiling(cast(harvest as float)) as int) as num_fish_harvested_unobserved_adjusted,
        cast(ceiling(cast(harvest_unadj as float)) as int) as num_fish_harvested_unobserved_unadjusted,
        cast(ceiling(cast(release as float)) as int) as num_fish_released_adjusted,
        cast(ceiling(cast(release_unadj as float)) as int) as num_fish_released_unadjusted,
        cast(ceiling(cast(tot_cat as float)) as int) as total_number_fish_caught,
        round(cast(tot_len_a as double), 2) as total_length_fish_harvested_observed_mm,
        round(cast(tot_len_b1 as double), 2) as total_length_fish_harvested_unobserved_mm,
        round(cast(tot_len as double), 2) as total_length_fish_harvested_mm,
        round(cast(wgt_a as double), 2) as total_weight_fish_harvested_observed_kg,
        round(cast(wgt_b1 as double), 2) as total_weight_fish_harvested_unobserved_kg,
        round(cast(wgt_ab1 as double), 2) as total_weight_fish_harvested_kg,  
        
    from source

)

select * from renamed