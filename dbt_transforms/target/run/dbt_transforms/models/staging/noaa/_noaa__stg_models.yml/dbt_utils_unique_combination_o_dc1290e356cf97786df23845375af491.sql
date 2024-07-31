select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      





with validation_errors as (

    select
        survey_id, species_common_name, fish_weight_kg, fish_length_mm, survey_year
    from "noaa_dw"."analytics"."stg_noaa__sizes"
    group by survey_id, species_common_name, fish_weight_kg, fish_length_mm, survey_year
    having count(*) > 1

)

select *
from validation_errors



      
    ) dbt_internal_test