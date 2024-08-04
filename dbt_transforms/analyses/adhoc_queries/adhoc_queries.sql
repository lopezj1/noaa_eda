--get number of records in sources
select count(*) from {{ source('raw', 'catch') }}
select count(*) from {{ source('raw', 'size') }}
select count(*) from {{ source('raw', 'trip') }}

--get number of records in base models
select count(*) from {{ ref('base_noaa__catches') }}
select count(*) from {{ ref('base_noaa__sizes') }}
select count(*) from {{ ref('base_noaa__trips') }}

--get number of records in staging models
select count(*) from {{ ref('stg_noaa__catches') }}
select count(*) from {{ ref('stg_noaa__sizes') }}
select count(*) from {{ ref('stg_noaa__trips') }}

--get distinct survey ids in stg_noaa__trips
select count(survey_id) from {{ ref('stg_noaa__trips') }}
select count(distinct survey_id) from {{ ref('stg_noaa__trips') }}

--inspect records with duplicate id_code
with catch as (select * from {{ source('raw', 'catch') }})
select 
--count(*)
ID_CODE, WAVE, strat_id, date_published, AREA_X, kod, SUB_REG, ST, psu_id, MODE_FX, YEAR, *
from catch
where ID_CODE IN (select id_code from catch group by id_code having count(*) > 1)
order by id_code desc

with catch as (select * from {{ source('raw', 'size') }})
select 
--count(*)
ID_CODE, WAVE, strat_id, date_published, AREA_X, kod, SUB_REG, ST, psu_id, MODE_FX, YEAR, *
from catch
where ID_CODE IN (select id_code from catch group by id_code having count(*) > 1)
order by id_code desc

with catch as (select * from {{ source('raw', 'trip') }})
select 
--count(*)
ID_CODE, WAVE, strat_id, date_published, AREA_X, kod, SUB_REG, ST, psu_id, MODE_FX, YEAR, *
from catch
where ID_CODE IN (select id_code from catch group by id_code having count(*) > 1)
order by id_code desc

--get number of records in staging models
select count(*) from {{ ref('stg_noaa__catches') }}
select count(*) from {{ ref('stg_noaa__sizes') }}
select count(*) from {{ ref('stg_noaa__trips') }}

--find missing years
with date_spine as (

                    {{ dbt_utils.date_spine(
                        datepart="year",
                        start_date="cast('1981-01-01' as date)",
                        end_date="cast('2024-01-01' as date)"
                    )
                    }}

),

year_spine as (

                    SELECT year(date_year) as year
                    from date_spine

)

SELECT distinct year
FROM year_spine
LEFT JOIN {{ ref('stg_noaa__trips') }} c ON year_spine.year = c.trip_year
WHERE c.trip_year IS NULL

--get trips where target species identified and not identified and compare to total number of trips
select count(*) as num_trips_total, count(*) - count(species_common_name) as num_trips_without_target_species, count(species_common_name) as num_trips_with_target_species from {{ ref('stg_noaa__sizes') }}

--get top 10 species by number of trips were targeted
select sum(num_trips_where_species_targeted)
from
(
select 
species_common_name, 
count(species_common_name) as num_trips_where_species_targeted
from {{ ref('stg_noaa__sizes') }}
group by species_common_name
order by num_trips_where_species_targeted desc
limit 10
)

--create catch rate column partitioned by region
select caught, us_region, try_cast(caught as int) / sum(try_cast(caught as int)) over (partition by us_region) as catch_rate from {{ ref('stg_noaa__trips') }}

--
select count(*) from analytics.trip_details

-- investigate rows with same survey_id
select survey_id, count()
from {{ ref('stg_noaa__catches') }}
group by survey_id
order by count() desc

select * from {{ ref('stg_noaa__catches') }} 
where survey_id = 1644120190812013
order by species_common_name desc

select * from {{ ref('stg_noaa__sizes') }} 
where survey_id = 1636620190414005
order by species_common_name desc