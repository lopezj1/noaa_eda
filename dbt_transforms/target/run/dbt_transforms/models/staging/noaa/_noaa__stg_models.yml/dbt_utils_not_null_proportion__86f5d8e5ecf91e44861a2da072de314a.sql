select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      







with validation as (
  select
    
    sum(case when survey_year is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion
  from "noaa_dw"."analytics"."stg_noaa__catches"
  
),
validation_errors as (
  select
    
    not_null_proportion
  from validation
  where not_null_proportion < 0.75 or not_null_proportion > 1
)
select
  *
from validation_errors


      
    ) dbt_internal_test