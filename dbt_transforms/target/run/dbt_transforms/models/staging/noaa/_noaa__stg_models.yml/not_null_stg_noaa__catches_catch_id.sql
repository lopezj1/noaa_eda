select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select catch_id
from "noaa_dw"."analytics"."stg_noaa__catches"
where catch_id is null



      
    ) dbt_internal_test