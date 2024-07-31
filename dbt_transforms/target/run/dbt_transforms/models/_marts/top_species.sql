
  
    
    

    create  table
      "noaa_dw"."analytics"."top_species__dbt_tmp"
  
    as (
      with 
sizes as (

        select * from "noaa_dw"."analytics"."stg_noaa__sizes"

),

top_species as (

        select 
        species_common_name, 
        count(species_common_name) as num_trips_where_species_targeted
        from sizes
        group by species_common_name
        order by num_trips_where_species_targeted desc
        limit 10

)

select * from top_species
    );
  
  