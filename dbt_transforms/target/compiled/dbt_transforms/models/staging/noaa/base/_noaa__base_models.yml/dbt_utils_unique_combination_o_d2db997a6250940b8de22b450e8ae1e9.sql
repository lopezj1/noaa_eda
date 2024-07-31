





with validation_errors as (

    select
        id_code, common, tot_cat, tot_len, year
    from "noaa_dw"."analytics"."base_noaa__catches"
    group by id_code, common, tot_cat, tot_len, year
    having count(*) > 1

)

select *
from validation_errors


