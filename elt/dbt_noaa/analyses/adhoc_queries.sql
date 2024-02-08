--get unique id_code that match 16 character format
select count(*) from {{ ref('stg_noaa__catches') }}
select count(*) from {{ ref('stg_noaa__sizes') }}
select count(*) from {{ ref('stg_noaa__trips') }}

select * from {{ ref('stg_noaa__catches') }}
select * from {{ ref('stg_noaa__sizes') }}
select * from {{ ref('stg_noaa__trips') }} where trip_date = '1982-01-30'

select column_name from information_schema.columns where table_name = 'stg_noaa__catches'
select column_name from information_schema.columns where table_name = 'stg_noaa__sizes'
select column_name from information_schema.columns where table_name = 'stg_noaa__trips'

select 
species_common_name, 
count(species_common_name) as num_trips_where_species_targeted
from {{ ref('stg_noaa__sizes') }}
group by species_common_name
order by num_trips_where_species_targeted desc
limit 10
