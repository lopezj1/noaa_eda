





with validation_errors as (

    select
        survey_id, survey_year
    from "noaa_dw"."analytics"."stg_noaa__trips"
    group by survey_id, survey_year
    having count(*) > 1

)

select *
from validation_errors


