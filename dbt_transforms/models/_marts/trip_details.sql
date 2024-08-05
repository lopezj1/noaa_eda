with trips as (

        select * from {{ ref('int_trips_fips_join') }}

),

catches as (

        select * from {{ ref('stg_noaa__catches') }}

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
        c.species_common_name,
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

)

select * from trip_details