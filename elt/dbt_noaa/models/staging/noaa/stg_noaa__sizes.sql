with source as (

    select * from {{ source('raw', 'size') }}

),

renamed as (

    select
        var_id,
        alt_flag,
        date_published,
        mode_fx,
        area_x,
        st,
        sub_reg,
        wave,
        kod,
        lngth_imp,
        wgt_imp,
        strat_id,
        wp_size,
        id_code,
        year

    from source

)

select * from renamed