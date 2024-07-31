





with validation_errors as (

    select
        id_code, year
    from "noaa_dw"."analytics"."base_noaa__trips"
    group by id_code, year
    having count(*) > 1

)

select *
from validation_errors


