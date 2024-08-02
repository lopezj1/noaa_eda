





with drop_cols as (
    -- drop cols with high null proportion
    select
    "common",
  "strat_id",
  "psu_id",
  "year",
  "st",
  "mode_fx",
  "area_x",
  "id_code",
  "sub_reg",
  "wave",
  "kod",
  "sp_code",
  "claim",
  "release",
  "harvest",
  "claim_unadj",
  "harvest_unadj",
  "release_unadj",
  "tot_len_a",
  "wgt_a",
  "tot_len_b1",
  "wgt_b1",
  "month",
  "tot_cat",
  "wgt_ab1",
  "tot_len",
  "landing",
  "var_id",
  "alt_flag",
  "fl_reg",
  "wp_int",
  "wp_catch",
  "date_published"
    from "noaa_dw"."raw"."catch"

)

select * from drop_cols

valid_records as (
    -- filter records where id_code is valid
    -- how to pass a cte as a relation to this macro look at the source code for deduplication macro
    

    with unfiltered as (

        select * from "noaa_dw"."raw"."catch"
    ),

    filtered as (

        select * from unfiltered
        where regexp_matches(id_code, '[0-9]{16}')

    ),

    fixed as (

        select * replace(regexp_replace(id_code, '[^0-9]', '') as id_code) from filtered

    )

    select * from fixed



)

select * from valid_records