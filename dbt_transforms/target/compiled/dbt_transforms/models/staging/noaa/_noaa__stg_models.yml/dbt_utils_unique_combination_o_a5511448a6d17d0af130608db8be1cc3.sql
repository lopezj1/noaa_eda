





with validation_errors as (

    select
        survey_id, species_common_name, total_number_fish_caught, total_length_fish_harvested_mm, survey_year
    from "noaa_dw"."analytics"."stg_noaa__catches"
    group by survey_id, species_common_name, total_number_fish_caught, total_length_fish_harvested_mm, survey_year
    having count(*) > 1

)

select *
from validation_errors


