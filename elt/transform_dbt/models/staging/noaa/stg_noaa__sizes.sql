{% set src = source('raw', 'size') %}
{% set null_proportion = 0.75 %}
{% set id_column = 'id_code_1' %}
{% set match_pattern = '[0-9]{16}' %}
{% set replace_pattern = '[^0-9]' %}

with drop_cols as (

    select
    {{ dbt_utils.star(from=src, except=drop_cols_high_nulls(src, null_proportion)) }}
    from {{ src }}

),

valid_records as (

    {{ filter_id_code('drop_cols', id_column, match_pattern, replace_pattern) }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['id_code_1','common','wgt','lngth','year_1']) }} as size_id,
        try_cast(id_code_1 as bigint) as survey_id,
        try_cast(strptime(date_published, '%m/%d/%Y') as date) as data_publish_date,
        try_cast(year_1 as int) as survey_year,
        try_cast(substring(id_code_1, 6, 4) as int) as trip_year,
        try_cast(substring(id_code_1, 10, 2) as int) trip_month_num,
        try_cast(substring(id_code_1, 12, 2) as int) as trip_day_num,
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
            when wave_1 = '1' then 'January/February'
            when wave_1 = '2' then 'March/April'
            when wave_1 = '3' then 'May/June'
            when wave_1 = '4' then 'July/August'
            when wave_1 = '5' then 'September/October'
            when wave_1 = '6' then 'November/December'
            else NULL
        end as sampling_period,
        case 
            when kod = 'wd' then false
            when kod = 'we' then true
            else NULL
        end as weekend,
        case 
            when sub_reg_1 = '4' then 'North Atlantic (ME; NH; MA; RI; CT)'
            when sub_reg_1 = '5' then 'Mid-Atlantic (NY; NJ; DE; MD; VA) '
            when sub_reg_1 = '6' then 'South Atlantic (NC; SC; GA; EFL)'
            when sub_reg_1 = '7' then 'Gulf of Mexico (WFL; AL; MS; LA)'
            when sub_reg_1 = '8' then 'West Pacific (HI)'
            when sub_reg_1 = '11' then 'U.S. Caribbean (Puerto Rico and Virgin Islands)'
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
            when mode_fx_1 = '1' then 'Man-Made'
            when mode_fx_1 = '2' then 'Beach/Bank'
            when mode_fx_1 = '3' then 'Shore'
            when mode_fx_1 = '4' then 'Headboat'
            when mode_fx_1 = '5' then 'Charter Boat'
            when mode_fx_1 = '6' then 'Charter Boat'
            when mode_fx_1 = '7' then 'Private/Rental Boat'
            else NULL
        end as fishing_method_collapsed,
        try_cast(st_1 as int) as state_code_where_caught,
        try_cast(common as varchar) as species_common_name,
        round(try_cast(wgt as double), 2) as fish_weight_kg,
        round(fish_weight_kg * 2.20462) as fish_weight_lbs,
        try_cast(wgt_imp as boolean) as imputed_weight,
        round(try_cast(lngth as double), 2) as fish_length_mm,        
        round(try_cast(l_cm_bin as double), 2) as fish_length_cm,        
        round(try_cast(l_in_bin as double), 2) as fish_length_in,
        try_cast(lngth_imp as boolean) as imputed_length

    from valid_records

),

deduplicated as (

{{ dbt_utils.snowflake__deduplicate(
    relation='renamed',
    partition_by='size_id',
    order_by='survey_year desc',
   )
}}

)

select * from deduplicated