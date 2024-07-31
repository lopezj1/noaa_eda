select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      





with validation_errors as (

    select
        survey_id, survey_year
    from "noaa_dw"."analytics"."stg_noaa__trips"
    group by survey_id, survey_year
    having count(*) > 1

)

select *
from validation_errors



      
    ) dbt_internal_test