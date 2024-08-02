
  
  create view "noaa_dw"."analytics"."stg_noaa__catches__dbt_tmp" as (
    





--drop columns with high null count
with drop_cols as (

    select
    "common",
  "strat_id",
  "psu_id",
  "year",
  "st",
  "mode_fx",
  "area_x",
  "id_code",
  "sub_reg",
  "wave",
  "kod",
  "sp_code",
  "claim",
  "release",
  "harvest",
  "claim_unadj",
  "harvest_unadj",
  "release_unadj",
  "tot_len_a",
  "wgt_a",
  "tot_len_b1",
  "wgt_b1",
  "month",
  "tot_cat",
  "wgt_ab1",
  "tot_len",
  "landing",
  "var_id",
  "alt_flag",
  "fl_reg",
  "wp_int",
  "wp_catch",
  "date_published"
    from "noaa_dw"."analytics"."base_noaa__catches"

),

--drop rows where id is not valid
valid_records as (

    

    with unfiltered as (

        select * from drop_cols
    ),

    filtered as (

        select * from unfiltered
        where regexp_matches(id_code, '[0-9]{16}')

    ),

    fixed as (

        select * replace(regexp_replace(id_code, '[^0-9]', '') as id_code) from filtered

    )

    select * from fixed



),

renamed as (

    select
        md5(cast(coalesce(cast(id_code as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(common as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(tot_cat as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(tot_len as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(year as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as catch_id,
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
        try_cast(common as varchar) as species_common_name,
        try_cast(ceiling(try_cast(claim as float)) as int) as num_fish_harvested_observed_adjusted,
        try_cast(ceiling(try_cast(claim_unadj as float)) as int) as num_fish_harvested_observed_unadjusted,
        try_cast(ceiling(try_cast(harvest as float)) as int) as num_fish_harvested_unobserved_adjusted,
        try_cast(ceiling(try_cast(harvest_unadj as float)) as int) as num_fish_harvested_unobserved_unadjusted,
        try_cast(ceiling(try_cast(release as float)) as int) as num_fish_released_adjusted,
        try_cast(ceiling(try_cast(release_unadj as float)) as int) as num_fish_released_unadjusted,
        try_cast(ceiling(try_cast(tot_cat as float)) as int) as total_number_fish_caught,
        round(try_cast(tot_len_a as double), 2) as total_length_fish_harvested_observed_mm,
        round(try_cast(tot_len_b1 as double), 2) as total_length_fish_harvested_unobserved_mm,
        round(try_cast(tot_len as double), 2) as total_length_fish_harvested_mm,
        round(try_cast(wgt_a as double), 2) as total_weight_fish_harvested_observed_kg,
        round(try_cast(wgt_b1 as double), 2) as total_weight_fish_harvested_unobserved_kg,
        round(try_cast(wgt_ab1 as double), 2) as total_weight_fish_harvested_kg,  
        
    from valid_records

),

--remove duplicates
deduplicated as (

select *
    from renamed
    qualify
        row_number() over (
            partition by catch_id
            order by survey_year desc
        ) = 1

)

select * from deduplicated
  );
