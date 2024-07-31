





    

with source as (

    select * from {{ source('raw', 'catch') }}

),

renamed as (

    select

    from source

)

select * from renamed




    

with source as (

    select * from {{ source('raw', 'size') }}

),

renamed as (

    select

    from source

)

select * from renamed




    

with source as (

    select * from {{ source('raw', 'trip') }}

),

renamed as (

    select

    from source

)

select * from renamed


