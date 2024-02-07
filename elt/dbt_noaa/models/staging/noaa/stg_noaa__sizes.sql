with source as (

    select * from {{ source('raw', 'size') }}

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
        st AS state_code_where_caught,
        common as species_common_name,
        wgt as fish_weight_kg,
        wgt_imp as imputed_weight,
        l_in_bin as fish_length_in,
        l_cm_bin as fish_length_cm,
        lngth_imp as imputed_length

    from source

)

select * from renamed