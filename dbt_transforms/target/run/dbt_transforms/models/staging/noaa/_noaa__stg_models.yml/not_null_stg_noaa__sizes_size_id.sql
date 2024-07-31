select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select size_id
from "noaa_dw"."analytics"."stg_noaa__sizes"
where size_id is null



      
    ) dbt_internal_test