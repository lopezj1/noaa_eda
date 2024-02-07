with source as (

    select * from {{ source('raw', 'trip') }}

),

renamed as (

    select
        cast(id_code as bigint) as noaa_id,
        cast(strptime(date_published, '%m/%d/%Y') as date) as data_publish_date,
        cast(substring(id_code, 6, 4) as int) as trip_year,
        cast(substring(id_code, 10, 2) as int) trip_month_num,
        cast(substring(id_code, 12, 2) as int) as trip_day_num,
        make_date(trip_year, trip_month_num, trip_day_num) as trip_date,
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
        year as survey_year,
        case 
            when kod = 'wd' then 0
            when kod = 'we' then 1
            else NULL
        end as weekend,
        case
            when 
            cast(left(time, 2) as int) between 1 and 23
            and 
            cast(right(time, 2) as int) between 1 and 59
            then make_time(cast(left(time, 2) as int), cast(right(time, 2) as int), 0.0)
            else NULL
        end as fish_caught_time,
        make_timestamp(trip_year, trip_month_num, trip_day_num, date_part('hour', fish_caught_time), date_part('minute', fish_caught_time), date_part('second', fish_caught_time)) as fish_caught_datetime,
        case
            when date_part('hour', fish_caught_time) between 0 and 5 then 'Before Dawn'
            when date_part('hour', fish_caught_time) between 6 and 11 then 'Morning'
            when date_part('hour', fish_caught_time) between 12 and 17 then 'Afternoon'
            when date_part('hour', fish_caught_time) between 18 and 23 then 'After Dusk'
            else NULL
        end as fish_caught_time_of_day,
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
        case
            when mode_f = 1 then 'Pier/Dock'
            when mode_f = 2 then 'Jetty/Breakwater/Breachway'
            when mode_f = 3 then 'Bridge/Causeway'
            when mode_f = 4 then 'Other man-made'
            when mode_f = 5 then 'Beach/Bank'
            when mode_f = 6 then 'Head Boat'
            when mode_f = 7 then 'Charter Boat'
            when mode_f = 8 then 'Private/Rental Boat'
            else NULL
        end as fishing_method_uncollapsed,
        case
            when coastal = 'N' then 'Non-coastal county resident'
            when coastal = 'Y' then 'Coastal county resident'
            when coastal = 'O' then 'Out-of-State'
            else NULL
        end as fisherman_state_residency_status,
        st AS state_code_where_caught,
        st_res as state_code_where_fisherman_resides,
        cnty as county_code_where_caught,
        cnty_res as county_code_where_fisherman_resides,
        ffdays12 as number_of_outings_in_last_year,
        ffdays2 as number_of_outings_in_last_2_months,
        cntrbtrs as number_of_anglers_interviewed,
        hrsf as trip_fishing_effort_hours,
        case
            when catch = 1 or catch = 3 then 1
            when catch = 2 then 0
            else NULL
        end as caught

    from source

)

select * from renamed