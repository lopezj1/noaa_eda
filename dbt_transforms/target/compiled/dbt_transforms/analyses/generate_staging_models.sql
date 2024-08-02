





    

with source as (

    select * from {{ source('raw', 'catch') }}

),

renamed as (

    select
        common,
        strat_id,
        psu_id,
        year,
        st,
        mode_fx,
        area_x,
        id_code,
        sub_reg,
        wave,
        kod,
        sp_code,
        claim,
        release,
        harvest,
        claim_unadj,
        harvest_unadj,
        release_unadj,
        tot_len_a,
        wgt_a,
        tot_len_b1,
        wgt_b1,
        region,
        month,
        tot_cat,
        wgt_ab1,
        tot_len,
        landing,
        var_id,
        alt_flag,
        strat_interval,
        fl_reg,
        wp_catch_precal,
        wp_int,
        wp_catch,
        date_published,
        arx_method,
        imp_rec,
        _typex,
        _freqx,
        wp_int_precal

    from source

)

select * from renamed




    

with source as (

    select * from {{ source('raw', 'size') }}

),

renamed as (

    select
        year,
        st,
        mode_fx,
        area_x,
        id_code,
        sub_reg,
        wave,
        month,
        kod,
        sp_code,
        lngth,
        wgt,
        lngth_imp,
        wgt_imp,
        strat_id,
        psu_id,
        common,
        wp_size,
        l_in_bin,
        l_cm_bin,
        var_id,
        alt_flag,
        date_published,
        wgt_unadj,
        imp_rec

    from source

)

select * from renamed




    

with source as (

    select * from {{ source('raw', 'trip') }}

),

renamed as (

    select
        prim2_common,
        prim1_common,
        strat_id,
        psu_id,
        add_hrs,
        area,
        area_x,
        catch,
        cntrbtrs,
        cnty,
        cnty_res,
        coastal,
        ffdays2,
        ffdays12,
        hrsf,
        id_code,
        intsite,
        mode_f,
        mode_fx,
        num_typ2,
        num_typ3,
        num_typ4,
        reg_res,
        st,
        st_res,
        sub_reg,
        telefon,
        wave,
        year,
        asg_code,
        month,
        kod,
        prt_code,
        celltype,
        fshinsp_a,
        num_fish_a,
        fl_reg,
        add_ph,
        county,
        date1,
        dist,
        f_by_p,
        gear,
        prim1,
        prim2,
        pvt_res,
        rig,
        sep_fish,
        time,
        age,
        wp_int,
        var_id,
        alt_flag,
        leader,
        date_published,
        first,
        num_typ6,
        on_list,
        party,
        zip,
        area_nc,
        boat_hrs,
        mode2001,
        muni_res,
        num_typ9,
        new_list,
        mode_asg,
        tsn1,
        tsn2,
        distkeys,
        license,
        monitor,
        compflag,
        art_reef,
        gender,
        tourn,
        turtle,
        date,
        region,
        strat_interval,
        reefcode,
        wp_int_precal,
        imp_rec,
        muni_trp,
        arx_method,
        reef_code,
        reef

    from source

)

select * from renamed


