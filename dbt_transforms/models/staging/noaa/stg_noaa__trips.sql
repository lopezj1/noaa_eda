{% set relation = ref('base_noaa__trips') %}
{% set null_proportion = 0.75 %}
{% set id_column = 'id_code' %}
{% set match_pattern = '[0-9]{16}' %}
{% set replace_pattern = '[^0-9]' %}

--drop columns with high null count
with drop_cols as (

    select
    {{ dbt_utils.star(from=relation, except=drop_cols_high_nulls(relation, null_proportion)) }}
    from {{ relation }}

),

--drop rows where id is not valid
valid_records as (

    {{ filter_id_code('drop_cols', id_column, match_pattern, replace_pattern) }}

),

renamed as (

    select
        --{{ dbt_utils.generate_surrogate_key(['id_code', 'year']) }} as trip_id,
        try_cast(id_code as bigint) as survey_id,
        try_cast(strptime(date_published, '%m/%d/%Y') as date) as data_publish_date,
        try_cast(year as int) as survey_year,
        try_cast(substring(id_code, 6, 4) as int) as trip_year,
        try_cast(substring(id_code, 10, 2) as int) trip_month_num,
        try_cast(substring(id_code, 12, 2) as int) as trip_day_num,
        case
            when 
            coalesce(trip_month_num, 0) in (1,3,5,7,8,10,12) and trip_day_num between 1 and 31
            or
            coalesce(trip_month_num, 0) in (4,6,9,11) and trip_day_num between 1 and 30
            or
            coalesce(trip_month_num, 0) = 2 and trip_day_num between 1 and 29
            then make_date(trip_year, trip_month_num, trip_day_num) 
            else NULL
        end as trip_date,
        dayname(trip_date) as trip_day_of_week,
        monthname(trip_date) as trip_month_name,
        case
            when trip_month_name in ('December', 'January', 'February') then 'Winter'
            when trip_month_name in ('March', 'April', 'May') then 'Spring'
            when trip_month_name in ('June', 'July', 'August') then 'Summer'
            when trip_month_name in ('September', 'October', 'November') then 'Fall'
            else NULL
        end as fishing_season,
        case
            when wave = '1' then 'January/February'
            when wave = '2' then 'March/April'
            when wave = '3' then 'May/June'
            when wave = '4' then 'July/August'
            when wave = '5' then 'September/October'
            when wave = '6' then 'November/December'
            else NULL
        end as sampling_period,
        case 
            when kod = 'wd' then false
            when kod = 'we' then true
            else NULL
        end as weekend,
        case 
            when sub_reg = '4' then 'North Atlantic (ME; NH; MA; RI; CT)'
            when sub_reg = '5' then 'Mid-Atlantic (NY; NJ; DE; MD; VA) '
            when sub_reg = '6' then 'South Atlantic (NC; SC; GA; EFL)'
            when sub_reg = '7' then 'Gulf of Mexico (WFL; AL; MS; LA)'
            when sub_reg = '8' then 'West Pacific (HI)'
            when sub_reg = '11' then 'U.S. Caribbean (Puerto Rico and Virgin Islands)'
            else NULL
        end as us_region,
        case
            when area_x = '1' then 'Ocean - Within 3 miles'
            when area_x = '2' then 'Ocean - Outside 3 miles'
            when area_x = '3' then 'Ocean - Within 10 miles'
            when area_x = '4' then 'Ocean - Outside 10 miles'
            when area_x = '5' then 'Inland'
            else NULL
        end as nautical_zone,
        case 
            when mode_fx = '1' then 'Man-Made'
            when mode_fx = '2' then 'Beach/Bank'
            when mode_fx = '3' then 'Shore'
            when mode_fx = '4' then 'Headboat'
            when mode_fx = '5' then 'Charter Boat'
            when mode_fx = '6' then 'Charter Boat'
            when mode_fx = '7' then 'Private/Rental Boat'
            else NULL
        end as fishing_method_collapsed,
        try_cast(lpad(st, 2, '0') as int) as state_code_where_caught,
        try_cast(lpad(cnty, 3, '0') as int) as county_code_where_caught,
        try_cast(concat(cast(state_code_where_caught as varchar), cast(county_code_where_caught as varchar)) as int) as fips_code_where_caught,
        try_cast(lpad(st_res, 2, '0') as int) as state_code_where_fisherman_resides,
        try_cast(lpad(cnty_res, 3, '0') as int) as county_code_where_fisherman_resides,
        try_cast(concat(cast(state_code_where_fisherman_resides as varchar), cast(county_code_where_fisherman_resides as varchar)) as int) as fips_code_where_fisherman_resides,
        case
            when coastal = 'N' then 'Non-coastal county resident'
            when coastal = 'Y' then 'Coastal county resident'
            when coastal = 'O' then 'Out-of-State'
            else NULL
        end as fisherman_state_residency_status,
        case
            when mode_f = '1' then 'Pier/Dock'
            when mode_f = '2' then 'Jetty/Breakwater/Breachway'
            when mode_f = '3' then 'Bridge/Causeway'
            when mode_f = '4' then 'Other man-made'
            when mode_f = '5' then 'Beach/Bank'
            when mode_f = '6' then 'Head Boat'
            when mode_f = '7' then 'Charter Boat'
            when mode_f = '8' then 'Private/Rental Boat'
            else NULL
        end as fishing_method_uncollapsed,
        try_cast(ffdays12 as int) as number_of_outings_in_last_year,
        try_cast(ffdays2 as int) as number_of_outings_in_last_2_months,
        try_cast(cntrbtrs as int) as number_of_anglers_interviewed,
        round(try_cast(hrsf as double), 2) as trip_fishing_effort_hours,
        case
            when catch = '1' or catch = '3' then true
            when catch = '2' then false
            else NULL
        end as caught,
        case
            when 
            try_cast(left(time, 2) as int) between 1 and 23
            and 
            try_cast(right(time, 2) as int) between 1 and 59
            then make_time(try_cast(left(time, 2) as int), try_cast(right(time, 2) as int), 0.0)
            else NULL
        end as fish_caught_time,
        make_timestamp(date_part('year', trip_date), 
                        date_part('month', trip_date), 
                        date_part('day', trip_date), 
                        date_part('hour', fish_caught_time), 
                        date_part('minute', fish_caught_time), 
                        date_part('second', fish_caught_time)
                        ) as fish_caught_datetime,
        case
            when date_part('hour', fish_caught_time) between 0 and 5 then 'Before Dawn'
            when date_part('hour', fish_caught_time) between 6 and 11 then 'Morning'
            when date_part('hour', fish_caught_time) between 12 and 17 then 'Afternoon'
            when date_part('hour', fish_caught_time) between 18 and 23 then 'After Dusk'
            else NULL
        end as fish_caught_time_of_day

    from valid_records

),

--remove duplicates
deduplicated as (

{# {{ dbt_utils.snowflake__deduplicate(
    relation='renamed',
    partition_by='trip_id',
    order_by='survey_year desc',
   )
}} #}

{{ dbt_utils.snowflake__deduplicate(
    relation='renamed',
    partition_by='survey_id',
    order_by='data_publish_date desc, survey_year desc'
   )
}}

)

select * from deduplicated

