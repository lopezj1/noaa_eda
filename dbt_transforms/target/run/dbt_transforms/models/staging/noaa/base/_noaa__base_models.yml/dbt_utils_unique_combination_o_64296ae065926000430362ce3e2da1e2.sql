select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      





with validation_errors as (

    select
        id_code, common, wgt, lngth, year
    from "noaa_dw"."analytics"."base_noaa__sizes"
    group by id_code, common, wgt, lngth, year
    having count(*) > 1

)

select *
from validation_errors



      
    ) dbt_internal_test