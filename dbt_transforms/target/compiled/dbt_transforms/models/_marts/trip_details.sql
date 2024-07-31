with  __dbt__cte__int_trips_fips_join as (
with trips as (

        select * from "noaa_dw"."analytics"."stg_noaa__trips"

),

state_fips_codes as (

        select * from "noaa_dw"."analytics"."state_fips_codes"

),

county_fips_codes as (

        select * from "noaa_dw"."analytics"."county_fips_codes"

),

trips_fips_joined as (

        select
        t.survey_id,
        t.data_publish_date,
        t.trip_year,
        t.trip_month_num,
        t.trip_day_num,
        t.trip_date,
        t.trip_day_of_week,
        t.trip_month_name,
        t.fishing_season,
        t.sampling_period,
        t.weekend,
        t.us_region,
        t.nautical_zone,
        t.fishing_method_collapsed,
        t.state_code_where_caught,
        s2.state_name as state_where_caught,
        t.county_code_where_caught,
        c2.county_name as county_where_caught,
        t.fips_code_where_caught,
        t.state_code_where_fisherman_resides,
        s1.state_name as state_where_fisherman_resides,
        t.county_code_where_fisherman_resides,
        c1.county_name as county_where_fisherman_resides,
        t.fips_code_where_fisherman_resides,
        t.fisherman_state_residency_status,
        t.fishing_method_uncollapsed,
        t.number_of_outings_in_last_year,
        t.number_of_outings_in_last_2_months,
        t.number_of_anglers_interviewed,
        t.trip_fishing_effort_hours,
        t.caught,
        t.fish_caught_time,
        t.fish_caught_datetime,
        t.fish_caught_time_of_day
        from trips t
        left join county_fips_codes c1 on c1.county_fips = t.fips_code_where_fisherman_resides
        left join county_fips_codes c2 on c2.county_fips = t.fips_code_where_caught
        left join state_fips_codes s1 on s1.state_fips = t.state_code_where_fisherman_resides
        left join state_fips_codes s2 on s2.state_fips = t.state_code_where_caught

)

select * from trips_fips_joined
), catches as (

        select * from "noaa_dw"."analytics"."stg_noaa__catches"

),

sizes as (

        select * from "noaa_dw"."analytics"."stg_noaa__sizes"

),

trips as (

        select * from __dbt__cte__int_trips_fips_join

),

trip_details as (
        
        select
        t.survey_id,
        t.data_publish_date,
        t.fish_caught_time,
        t.fish_caught_datetime,
        t.fish_caught_time_of_day,
        t.trip_year,
        t.trip_month_num,
        t.trip_day_num,
        t.trip_date,
        t.trip_day_of_week,
        t.trip_month_name,
        t.fishing_season,
        t.sampling_period,
        t.weekend,
        t.us_region,
        t.nautical_zone,
        t.fishing_method_collapsed,
        t.state_where_caught,
        t.state_where_fisherman_resides,
        t.county_where_caught,
        t.county_where_fisherman_resides,
        t.fisherman_state_residency_status,
        t.fishing_method_uncollapsed,
        t.number_of_outings_in_last_year,
        t.number_of_outings_in_last_2_months,
        t.number_of_anglers_interviewed,
        t.trip_fishing_effort_hours,
        t.caught,
        s.species_common_name,
        s.fish_weight_kg,
        s.imputed_weight,
        s.fish_length_in,
        s.fish_length_cm,
        s.imputed_length,
        c.num_fish_harvested_observed_adjusted,
        c.num_fish_harvested_observed_unadjusted,
        c.num_fish_harvested_unobserved_adjusted,
        c.num_fish_harvested_unobserved_unadjusted,
        c.num_fish_released_adjusted,
        c.num_fish_released_unadjusted,
        c.total_number_fish_caught,
        c.total_length_fish_harvested_observed_mm,
        c.total_length_fish_harvested_unobserved_mm,
        c.total_length_fish_harvested_mm,
        c.total_weight_fish_harvested_observed_kg,
        c.total_weight_fish_harvested_unobserved_kg,
        c.total_weight_fish_harvested_kg
        from trips t
        left join catches c on c.survey_id = t.survey_id
        left join sizes s on s.survey_id = t.survey_id

)

select * from trip_details