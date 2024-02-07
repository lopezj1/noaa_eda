--get unique id_code that match 16 character format
select id_code, count(*) from {{ source('raw', 'catch') }} group by id_code having count(*) = 1 and len(id_code) = 16
select id_code, count(*) from {{ source('raw', 'size') }} group by id_code having count(*) = 1 and len(id_code) = 16
select id_code, count(*) from {{ source('raw', 'trip') }} group by id_code having count(*) = 1 and len(id_code) = 16

select noaa_id, count(*) from {{ ref('stg_noaa__catches') }} group by noaa_id having count(*) = 1 and len(noaa_id) = 16
select noaa_id, count(*) from {{ ref('stg_noaa__sizes') }} group by noaa_id having count(*) = 1 and len(noaa_id) = 16
select noaa_id, count(*) from {{ ref('stg_noaa__trips') }} group by noaa_id having count(*) = 1 and len(noaa_id) = 16

select * from {{ ref('stg_noaa__catches') }}
select * from {{ ref('stg_noaa__sizes') }}
select * from {{ ref('stg_noaa__trips') }}