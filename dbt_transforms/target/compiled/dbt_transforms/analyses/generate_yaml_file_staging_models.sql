

depends_on: "noaa_dw"."analytics"."stg_noaa__catches"
depends_on: "noaa_dw"."analytics"."stg_noaa__sizes"
depends_on: "noaa_dw"."analytics"."stg_noaa__trips"


    
    version: 2

models:
  - name: stg_noaa__sizes
    description: ""
    columns:
      - name: size_id
        data_type: varchar
        description: ""

      - name: survey_id
        data_type: bigint
        description: ""

      - name: data_publish_date
        data_type: date
        description: ""

      - name: survey_year
        data_type: integer
        description: ""

      - name: trip_year
        data_type: integer
        description: ""

      - name: trip_month_num
        data_type: integer
        description: ""

      - name: trip_day_num
        data_type: integer
        description: ""

      - name: trip_date
        data_type: date
        description: ""

      - name: trip_day_of_week
        data_type: varchar
        description: ""

      - name: trip_month_name
        data_type: varchar
        description: ""

      - name: fishing_season
        data_type: varchar
        description: ""

      - name: sampling_period
        data_type: varchar
        description: ""

      - name: weekend
        data_type: boolean
        description: ""

      - name: us_region
        data_type: varchar
        description: ""

      - name: nautical_zone
        data_type: varchar
        description: ""

      - name: fishing_method_collapsed
        data_type: varchar
        description: ""

      - name: state_code_where_caught
        data_type: integer
        description: ""

      - name: species_common_name
        data_type: varchar
        description: ""

      - name: fish_weight_kg
        data_type: double
        description: ""

      - name: fish_weight_lbs
        data_type: double
        description: ""

      - name: imputed_weight
        data_type: boolean
        description: ""

      - name: fish_length_mm
        data_type: double
        description: ""

      - name: fish_length_cm
        data_type: double
        description: ""

      - name: fish_length_in
        data_type: double
        description: ""

      - name: imputed_length
        data_type: boolean
        description: ""

  - name: stg_noaa__catches
    description: ""
    columns:
      - name: catch_id
        data_type: varchar
        description: ""

      - name: survey_id
        data_type: bigint
        description: ""

      - name: data_publish_date
        data_type: date
        description: ""

      - name: survey_year
        data_type: integer
        description: ""

      - name: trip_year
        data_type: integer
        description: ""

      - name: trip_month_num
        data_type: integer
        description: ""

      - name: trip_day_num
        data_type: integer
        description: ""

      - name: trip_date
        data_type: date
        description: ""

      - name: trip_day_of_week
        data_type: varchar
        description: ""

      - name: trip_month_name
        data_type: varchar
        description: ""

      - name: fishing_season
        data_type: varchar
        description: ""

      - name: sampling_period
        data_type: varchar
        description: ""

      - name: weekend
        data_type: boolean
        description: ""

      - name: us_region
        data_type: varchar
        description: ""

      - name: nautical_zone
        data_type: varchar
        description: ""

      - name: fishing_method_collapsed
        data_type: varchar
        description: ""

      - name: state_code_where_caught
        data_type: integer
        description: ""

      - name: species_common_name
        data_type: varchar
        description: ""

      - name: num_fish_harvested_observed_adjusted
        data_type: integer
        description: ""

      - name: num_fish_harvested_observed_unadjusted
        data_type: integer
        description: ""

      - name: num_fish_harvested_unobserved_adjusted
        data_type: integer
        description: ""

      - name: num_fish_harvested_unobserved_unadjusted
        data_type: integer
        description: ""

      - name: num_fish_released_adjusted
        data_type: integer
        description: ""

      - name: num_fish_released_unadjusted
        data_type: integer
        description: ""

      - name: total_number_fish_caught
        data_type: integer
        description: ""

      - name: total_length_fish_harvested_observed_mm
        data_type: double
        description: ""

      - name: total_length_fish_harvested_unobserved_mm
        data_type: double
        description: ""

      - name: total_length_fish_harvested_mm
        data_type: double
        description: ""

      - name: total_weight_fish_harvested_observed_kg
        data_type: double
        description: ""

      - name: total_weight_fish_harvested_unobserved_kg
        data_type: double
        description: ""

      - name: total_weight_fish_harvested_kg
        data_type: double
        description: ""

  - name: stg_noaa__trips
    description: ""
    columns:
      - name: trip_id
        data_type: varchar
        description: ""

      - name: survey_id
        data_type: bigint
        description: ""

      - name: data_publish_date
        data_type: date
        description: ""

      - name: survey_year
        data_type: integer
        description: ""

      - name: trip_year
        data_type: integer
        description: ""

      - name: trip_month_num
        data_type: integer
        description: ""

      - name: trip_day_num
        data_type: integer
        description: ""

      - name: trip_date
        data_type: date
        description: ""

      - name: trip_day_of_week
        data_type: varchar
        description: ""

      - name: trip_month_name
        data_type: varchar
        description: ""

      - name: fishing_season
        data_type: varchar
        description: ""

      - name: sampling_period
        data_type: varchar
        description: ""

      - name: weekend
        data_type: boolean
        description: ""

      - name: us_region
        data_type: varchar
        description: ""

      - name: nautical_zone
        data_type: varchar
        description: ""

      - name: fishing_method_collapsed
        data_type: varchar
        description: ""

      - name: state_code_where_caught
        data_type: integer
        description: ""

      - name: county_code_where_caught
        data_type: integer
        description: ""

      - name: fips_code_where_caught
        data_type: integer
        description: ""

      - name: state_code_where_fisherman_resides
        data_type: integer
        description: ""

      - name: county_code_where_fisherman_resides
        data_type: integer
        description: ""

      - name: fips_code_where_fisherman_resides
        data_type: integer
        description: ""

      - name: fisherman_state_residency_status
        data_type: varchar
        description: ""

      - name: fishing_method_uncollapsed
        data_type: varchar
        description: ""

      - name: number_of_outings_in_last_year
        data_type: integer
        description: ""

      - name: number_of_outings_in_last_2_months
        data_type: integer
        description: ""

      - name: number_of_anglers_interviewed
        data_type: integer
        description: ""

      - name: trip_fishing_effort_hours
        data_type: double
        description: ""

      - name: caught
        data_type: boolean
        description: ""

      - name: fish_caught_time
        data_type: time
        description: ""

      - name: fish_caught_datetime
        data_type: timestamp
        description: ""

      - name: fish_caught_time_of_day
        data_type: varchar
        description: ""

