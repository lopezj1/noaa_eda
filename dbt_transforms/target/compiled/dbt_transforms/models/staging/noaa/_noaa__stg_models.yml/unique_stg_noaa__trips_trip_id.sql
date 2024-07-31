
    
    

select
    trip_id as unique_field,
    count(*) as n_records

from "noaa_dw"."analytics"."stg_noaa__trips"
where trip_id is not null
group by trip_id
having count(*) > 1


