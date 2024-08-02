-- Returns a list of the columns from a relation, so you can then iterate in a for loop

--['year', 'st', 'mode_fx', 'area_x', 'id_code', 'sub_reg', 'wave', 'month', 'kod', 'sp_code', 'lngth', 'wgt', 'lngth_imp', 'wgt_imp', 'strat_id', 'psu_id', 'common', 'wp_size', 'l_in_bin', 'l_cm_bin', 'var_id', 'alt_flag', 'date_published', 'wgt_unadj', 'imp_rec']

-- Loop through column names and append any duplicated column names to a new list


--[]

-- Get the original column name and append to separate list


--[]

-- Merge the original and duplicated column name using coalesce
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
from "noaa_dw"."raw"."size"