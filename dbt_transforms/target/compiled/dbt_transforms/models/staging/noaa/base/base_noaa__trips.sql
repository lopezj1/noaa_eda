

with merge_cols as (

    

    -- Returns a list of the columns from a relation, so you can then iterate in a for loop

    --['prim2_common', 'prim1_common', 'strat_id', 'psu_id', 'add_hrs', 'area', 'area_x', 'catch', 'cntrbtrs', 'cnty', 'cnty_res', 'coastal', 'ffdays2', 'ffdays12', 'hrsf', 'id_code', 'intsite', 'mode_f', 'mode_fx', 'num_typ2', 'num_typ3', 'num_typ4', 'reg_res', 'st', 'st_res', 'sub_reg', 'telefon', 'wave', 'year', 'asg_code', 'month', 'kod', 'prt_code', 'celltype', 'fshinsp_a', 'num_fish_a', 'fl_reg', 'add_ph', 'county', 'date1', 'dist', 'f_by_p', 'gear', 'prim1', 'prim2', 'pvt_res', 'rig', 'sep_fish', 'time', 'age', 'wp_int', 'var_id', 'alt_flag', 'leader', 'date_published', 'first', 'num_typ6', 'on_list', 'party', 'zip', 'area_nc', 'boat_hrs', 'mode2001', 'muni_res', 'num_typ9', 'new_list', 'mode_asg', 'tsn1', 'tsn2', 'distkeys', 'license', 'monitor', 'compflag', 'art_reef', 'gender', 'tourn', 'turtle', 'date', 'region', 'strat_interval', 'reefcode', 'wp_int_precal', 'imp_rec', 'muni_trp', 'arx_method', 'reef_code', 'reef']

    -- Loop through column names and append any duplicated column names to a new list
    

    --[]

    -- Get the original column name and append to separate list
    

    --[]

    -- Merge the original and duplicated column name using coalesce
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
    from "noaa_dw"."raw"."trip"



)

select * from merge_cols