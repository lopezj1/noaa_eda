--get unique id_code that match 16 character format
select count(*) from {{ ref('stg_noaa__catches') }}
select count(*) from {{ ref('stg_noaa__sizes') }}
select count(*) from {{ ref('stg_noaa__trips') }}
select count(*) from {{ ref('int_noaa_all_join') }}

select * from {{ ref('stg_noaa__catches') }}
select * from {{ ref('stg_noaa__sizes') }}
select * from {{ ref('stg_noaa__trips') }}
select * from {{ ref('int_noaa_all_join') }}

select column_name from information_schema.columns where table_name = 'stg_noaa__catches'
select column_name from information_schema.columns where table_name = 'stg_noaa__sizes'
select column_name from information_schema.columns where table_name = 'stg_noaa__trips'

select column_name from information_schema.columns where table_name = 'int_noaa_all_join'