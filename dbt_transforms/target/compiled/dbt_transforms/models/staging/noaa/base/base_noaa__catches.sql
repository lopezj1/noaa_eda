

with merge_cols as (

    

    -- Returns a list of the columns from a relation, so you can then iterate in a for loop

    --['common', 'strat_id', 'psu_id', 'year', 'st', 'mode_fx', 'area_x', 'id_code', 'sub_reg', 'wave', 'kod', 'sp_code', 'claim', 'release', 'harvest', 'claim_unadj', 'harvest_unadj', 'release_unadj', 'tot_len_a', 'wgt_a', 'tot_len_b1', 'wgt_b1', 'region', 'month', 'tot_cat', 'wgt_ab1', 'tot_len', 'landing', 'var_id', 'alt_flag', 'strat_interval', 'fl_reg', 'wp_catch_precal', 'wp_int', 'wp_catch', 'date_published', 'arx_method', 'imp_rec', '_typex', '_freqx', 'wp_int_precal']

    -- Loop through column names and append any duplicated column names to a new list
    

    --[]

    -- Get the original column name and append to separate list
    

    --[]

    -- Merge the original and duplicated column name using coalesce
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
    from "noaa_dw"."raw"."catch"



)

select * from merge_cols