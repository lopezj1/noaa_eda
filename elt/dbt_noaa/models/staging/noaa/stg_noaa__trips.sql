with source as (

    select * from {{ source('raw', 'trip') }}

),

renamed as (

    select
        date_published,
        strat_id,
        mode_fx,
        mode_f,
        area_x,
        st,
        cnty,
        intsite,
        hrsf,
        ffdays12,
        ffdays2,
        cnty_res,
        st_res,
        cntrbtrs,
        num_typ2,
        num_typ3,
        num_typ4,
        sub_reg,
        wave,
        coastal,
        catch,
        asg_code,
        kod,
        wp_int,
        var_id,
        alt_flag,
        leader,
        id_code,
        year

    from source

)

select * from renamed