with source as (

    select * from {{ source('raw', 'catch') }}

),

renamed as (

    select
        date_published,
        strat_id,
        mode_fx,
        area_x,
        st,
        sub_reg,
        wave,
        kod,
        release,
        harvest,
        claim_unadj,
        harvest_unadj,
        release_unadj,
        tot_len_b1,
        fl_reg,
        wp_int,
        wp_catch,
        id_code,
        year

    from source

)

select * from renamed