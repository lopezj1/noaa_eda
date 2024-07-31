







with validation as (
  select
    
    sum(case when species_common_name is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion
  from "noaa_dw"."analytics"."stg_noaa__sizes"
  
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

