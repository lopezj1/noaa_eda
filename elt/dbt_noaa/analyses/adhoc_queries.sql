--get unique id_code that match 16 character format
select count(*) from {{ ref('stg_noaa__catches') }}
select count(*) from {{ ref('stg_noaa__sizes') }}
select count(*) from {{ ref('stg_noaa__trips') }} where caught is not null

select * from {{ ref('stg_noaa__catches') }}
select * from {{ ref('stg_noaa__sizes') }}
select * from {{ ref('stg_noaa__trips') }}

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

select caught, us_region, try_cast(caught as int) / sum(try_cast(caught as int)) over (partition by us_region) from {{ ref('stg_noaa__trips') }}
