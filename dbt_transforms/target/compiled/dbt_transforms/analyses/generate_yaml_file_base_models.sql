

depends_on: "noaa_dw"."analytics"."base_noaa__catches"
depends_on: "noaa_dw"."analytics"."base_noaa__sizes"
depends_on: "noaa_dw"."analytics"."base_noaa__trips"


    
    version: 2

models:
  - name: base_noaa__catches
    description: ""
    columns:
      - name: common
        data_type: varchar
        description: ""

      - name: strat_id
        data_type: varchar
        description: ""

      - name: psu_id
        data_type: varchar
        description: ""

      - name: year
        data_type: varchar
        description: ""

      - name: st
        data_type: varchar
        description: ""

      - name: mode_fx
        data_type: varchar
        description: ""

      - name: area_x
        data_type: varchar
        description: ""

      - name: id_code
        data_type: varchar
        description: ""

      - name: sub_reg
        data_type: varchar
        description: ""

      - name: wave
        data_type: varchar
        description: ""

      - name: kod
        data_type: varchar
        description: ""

      - name: sp_code
        data_type: varchar
        description: ""

      - name: claim
        data_type: varchar
        description: ""

      - name: release
        data_type: varchar
        description: ""

      - name: harvest
        data_type: varchar
        description: ""

      - name: claim_unadj
        data_type: varchar
        description: ""

      - name: harvest_unadj
        data_type: varchar
        description: ""

      - name: release_unadj
        data_type: varchar
        description: ""

      - name: tot_len_a
        data_type: varchar
        description: ""

      - name: wgt_a
        data_type: varchar
        description: ""

      - name: tot_len_b1
        data_type: varchar
        description: ""

      - name: wgt_b1
        data_type: varchar
        description: ""

      - name: region
        data_type: varchar
        description: ""

      - name: month
        data_type: varchar
        description: ""

      - name: tot_cat
        data_type: varchar
        description: ""

      - name: wgt_ab1
        data_type: varchar
        description: ""

      - name: tot_len
        data_type: varchar
        description: ""

      - name: landing
        data_type: varchar
        description: ""

      - name: var_id
        data_type: varchar
        description: ""

      - name: alt_flag
        data_type: varchar
        description: ""

      - name: strat_interval
        data_type: varchar
        description: ""

      - name: fl_reg
        data_type: varchar
        description: ""

      - name: wp_catch_precal
        data_type: varchar
        description: ""

      - name: wp_int
        data_type: varchar
        description: ""

      - name: wp_catch
        data_type: varchar
        description: ""

      - name: date_published
        data_type: varchar
        description: ""

      - name: arx_method
        data_type: varchar
        description: ""

      - name: imp_rec
        data_type: varchar
        description: ""

      - name: _typex
        data_type: varchar
        description: ""

      - name: _freqx
        data_type: varchar
        description: ""

      - name: wp_int_precal
        data_type: varchar
        description: ""

  - name: base_noaa__sizes
    description: ""
    columns:
      - name: year
        data_type: varchar
        description: ""

      - name: st
        data_type: varchar
        description: ""

      - name: mode_fx
        data_type: varchar
        description: ""

      - name: area_x
        data_type: varchar
        description: ""

      - name: id_code
        data_type: varchar
        description: ""

      - name: sub_reg
        data_type: varchar
        description: ""

      - name: wave
        data_type: varchar
        description: ""

      - name: month
        data_type: varchar
        description: ""

      - name: kod
        data_type: varchar
        description: ""

      - name: sp_code
        data_type: varchar
        description: ""

      - name: lngth
        data_type: varchar
        description: ""

      - name: wgt
        data_type: varchar
        description: ""

      - name: lngth_imp
        data_type: varchar
        description: ""

      - name: wgt_imp
        data_type: varchar
        description: ""

      - name: strat_id
        data_type: varchar
        description: ""

      - name: psu_id
        data_type: varchar
        description: ""

      - name: common
        data_type: varchar
        description: ""

      - name: wp_size
        data_type: varchar
        description: ""

      - name: l_in_bin
        data_type: varchar
        description: ""

      - name: l_cm_bin
        data_type: varchar
        description: ""

      - name: var_id
        data_type: varchar
        description: ""

      - name: alt_flag
        data_type: varchar
        description: ""

      - name: date_published
        data_type: varchar
        description: ""

      - name: wgt_unadj
        data_type: varchar
        description: ""

      - name: imp_rec
        data_type: varchar
        description: ""

  - name: base_noaa__trips
    description: ""
    columns:
      - name: prim2_common
        data_type: varchar
        description: ""

      - name: prim1_common
        data_type: varchar
        description: ""

      - name: strat_id
        data_type: varchar
        description: ""

      - name: psu_id
        data_type: varchar
        description: ""

      - name: add_hrs
        data_type: varchar
        description: ""

      - name: area
        data_type: varchar
        description: ""

      - name: area_x
        data_type: varchar
        description: ""

      - name: catch
        data_type: varchar
        description: ""

      - name: cntrbtrs
        data_type: varchar
        description: ""

      - name: cnty
        data_type: varchar
        description: ""

      - name: cnty_res
        data_type: varchar
        description: ""

      - name: coastal
        data_type: varchar
        description: ""

      - name: ffdays2
        data_type: varchar
        description: ""

      - name: ffdays12
        data_type: varchar
        description: ""

      - name: hrsf
        data_type: varchar
        description: ""

      - name: id_code
        data_type: varchar
        description: ""

      - name: intsite
        data_type: varchar
        description: ""

      - name: mode_f
        data_type: varchar
        description: ""

      - name: mode_fx
        data_type: varchar
        description: ""

      - name: num_typ2
        data_type: varchar
        description: ""

      - name: num_typ3
        data_type: varchar
        description: ""

      - name: num_typ4
        data_type: varchar
        description: ""

      - name: reg_res
        data_type: varchar
        description: ""

      - name: st
        data_type: varchar
        description: ""

      - name: st_res
        data_type: varchar
        description: ""

      - name: sub_reg
        data_type: varchar
        description: ""

      - name: telefon
        data_type: varchar
        description: ""

      - name: wave
        data_type: varchar
        description: ""

      - name: year
        data_type: varchar
        description: ""

      - name: asg_code
        data_type: varchar
        description: ""

      - name: month
        data_type: varchar
        description: ""

      - name: kod
        data_type: varchar
        description: ""

      - name: prt_code
        data_type: varchar
        description: ""

      - name: celltype
        data_type: varchar
        description: ""

      - name: fshinsp_a
        data_type: varchar
        description: ""

      - name: num_fish_a
        data_type: varchar
        description: ""

      - name: fl_reg
        data_type: varchar
        description: ""

      - name: add_ph
        data_type: varchar
        description: ""

      - name: county
        data_type: varchar
        description: ""

      - name: date1
        data_type: varchar
        description: ""

      - name: dist
        data_type: varchar
        description: ""

      - name: f_by_p
        data_type: varchar
        description: ""

      - name: gear
        data_type: varchar
        description: ""

      - name: prim1
        data_type: varchar
        description: ""

      - name: prim2
        data_type: varchar
        description: ""

      - name: pvt_res
        data_type: varchar
        description: ""

      - name: rig
        data_type: varchar
        description: ""

      - name: sep_fish
        data_type: varchar
        description: ""

      - name: time
        data_type: varchar
        description: ""

      - name: age
        data_type: varchar
        description: ""

      - name: wp_int
        data_type: varchar
        description: ""

      - name: var_id
        data_type: varchar
        description: ""

      - name: alt_flag
        data_type: varchar
        description: ""

      - name: leader
        data_type: varchar
        description: ""

      - name: date_published
        data_type: varchar
        description: ""

      - name: first
        data_type: varchar
        description: ""

      - name: num_typ6
        data_type: varchar
        description: ""

      - name: on_list
        data_type: varchar
        description: ""

      - name: party
        data_type: varchar
        description: ""

      - name: zip
        data_type: varchar
        description: ""

      - name: area_nc
        data_type: varchar
        description: ""

      - name: boat_hrs
        data_type: varchar
        description: ""

      - name: mode2001
        data_type: varchar
        description: ""

      - name: muni_res
        data_type: varchar
        description: ""

      - name: num_typ9
        data_type: varchar
        description: ""

      - name: new_list
        data_type: varchar
        description: ""

      - name: mode_asg
        data_type: varchar
        description: ""

      - name: tsn1
        data_type: varchar
        description: ""

      - name: tsn2
        data_type: varchar
        description: ""

      - name: distkeys
        data_type: varchar
        description: ""

      - name: license
        data_type: varchar
        description: ""

      - name: monitor
        data_type: varchar
        description: ""

      - name: compflag
        data_type: varchar
        description: ""

      - name: art_reef
        data_type: varchar
        description: ""

      - name: gender
        data_type: varchar
        description: ""

      - name: tourn
        data_type: varchar
        description: ""

      - name: turtle
        data_type: varchar
        description: ""

      - name: date
        data_type: varchar
        description: ""

      - name: region
        data_type: varchar
        description: ""

      - name: strat_interval
        data_type: varchar
        description: ""

      - name: reefcode
        data_type: varchar
        description: ""

      - name: wp_int_precal
        data_type: varchar
        description: ""

      - name: imp_rec
        data_type: varchar
        description: ""

      - name: muni_trp
        data_type: varchar
        description: ""

      - name: arx_method
        data_type: varchar
        description: ""

      - name: reef_code
        data_type: varchar
        description: ""

      - name: reef
        data_type: varchar
        description: ""

