-- stg_noaa_mrip__catches.sql

with

source as (

    select * from {{ source('noaa_mrip','catch') }}

),

renamed as (

    select *

    from source

)

select * from renamed