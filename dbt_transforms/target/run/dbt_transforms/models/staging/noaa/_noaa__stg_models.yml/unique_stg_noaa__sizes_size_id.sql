select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    size_id as unique_field,
    count(*) as n_records

from "noaa_dw"."analytics"."stg_noaa__sizes"
where size_id is not null
group by size_id
having count(*) > 1



      
    ) dbt_internal_test