--get number of records in sources
select count(*) from "noaa_dw"."raw"."catch"
select count(*) from "noaa_dw"."raw"."size"
select count(*) from "noaa_dw"."raw"."trip"

--get number of records in base models
select count(*) from "noaa_dw"."analytics"."base_noaa__catches"
select count(*) from "noaa_dw"."analytics"."base_noaa__sizes"
select count(*) from "noaa_dw"."analytics"."base_noaa__trips"

--get number of records in staging models
select count(*) from "noaa_dw"."analytics"."stg_noaa__catches"
select count(*) from "noaa_dw"."analytics"."stg_noaa__sizes"
select count(*) from "noaa_dw"."analytics"."stg_noaa__trips"

--get distinct survey ids in stg_noaa__trips
select count(survey_id) from "noaa_dw"."analytics"."stg_noaa__trips"
select count(distinct survey_id) from "noaa_dw"."analytics"."stg_noaa__trips"

--inspect records with duplicate id_code
with catch as (select * from "noaa_dw"."raw"."catch")
select 
--count(*)
ID_CODE, WAVE, strat_id, date_published, AREA_X, kod, SUB_REG, ST, psu_id, MODE_FX, YEAR, *
from catch
where ID_CODE IN (select id_code from catch group by id_code having count(*) > 1)
order by id_code desc

with catch as (select * from "noaa_dw"."raw"."size")
select 
--count(*)
ID_CODE, WAVE, strat_id, date_published, AREA_X, kod, SUB_REG, ST, psu_id, MODE_FX, YEAR, *
from catch
where ID_CODE IN (select id_code from catch group by id_code having count(*) > 1)
order by id_code desc

with catch as (select * from "noaa_dw"."raw"."trip")
select 
--count(*)
ID_CODE, WAVE, strat_id, date_published, AREA_X, kod, SUB_REG, ST, psu_id, MODE_FX, YEAR, *
from catch
where ID_CODE IN (select id_code from catch group by id_code having count(*) > 1)
order by id_code desc

--get number of records in staging models
select count(*) from "noaa_dw"."analytics"."stg_noaa__catches"
select count(*) from "noaa_dw"."analytics"."stg_noaa__sizes"
select count(*) from "noaa_dw"."analytics"."stg_noaa__trips"

--find missing years
with date_spine as (

                    





with rawdata as (

    

    

    with p as (
        select 0 as generated_number union all select 1
    ), unioned as (

    select

    
    p0.generated_number * power(2, 0)
     + 
    
    p1.generated_number * power(2, 1)
     + 
    
    p2.generated_number * power(2, 2)
     + 
    
    p3.generated_number * power(2, 3)
     + 
    
    p4.generated_number * power(2, 4)
     + 
    
    p5.generated_number * power(2, 5)
    
    
    + 1
    as generated_number

    from

    
    p as p0
     cross join 
    
    p as p1
     cross join 
    
    p as p2
     cross join 
    
    p as p3
     cross join 
    
    p as p4
     cross join 
    
    p as p5
    
    

    )

    select *
    from unioned
    where generated_number <= 43
    order by generated_number



),

all_periods as (

    select (
        

    date_add(cast('1981-01-01' as date), interval (row_number() over (order by 1) - 1) year)


    ) as date_year
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_year <= cast('2024-01-01' as date)

)

select * from filtered



),

year_spine as (

                    SELECT year(date_year) as year
                    from date_spine

)

SELECT distinct year
FROM year_spine
LEFT JOIN "noaa_dw"."analytics"."stg_noaa__trips" c ON year_spine.year = c.trip_year
WHERE c.trip_year IS NULL

--get trips where target species identified and not identified and compare to total number of trips
select count(*) as num_trips_total, count(*) - count(species_common_name) as num_trips_without_target_species, count(species_common_name) as num_trips_with_target_species from "noaa_dw"."analytics"."stg_noaa__sizes"

--get top 10 species by number of trips were targeted
select sum(num_trips_where_species_targeted)
from
(
select 
species_common_name, 
count(species_common_name) as num_trips_where_species_targeted
from "noaa_dw"."analytics"."stg_noaa__sizes"
group by species_common_name
order by num_trips_where_species_targeted desc
limit 10
)

--create catch rate column partitioned by region
select caught, us_region, try_cast(caught as int) / sum(try_cast(caught as int)) over (partition by us_region) as catch_rate from "noaa_dw"."analytics"."stg_noaa__trips"

--
select count(*) from analytics.trip_details