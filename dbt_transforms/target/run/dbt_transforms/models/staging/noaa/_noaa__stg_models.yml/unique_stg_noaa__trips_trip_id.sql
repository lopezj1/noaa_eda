select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    trip_id as unique_field,
    count(*) as n_records

from "noaa_dw"."analytics"."stg_noaa__trips"
where trip_id is not null
group by trip_id
having count(*) > 1



      
    ) dbt_internal_test