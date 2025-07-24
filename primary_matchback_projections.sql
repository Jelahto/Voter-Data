with current_status as(
select *

from democrats.av_ev_wa_20240806_primary.WA_statewide_46_source

)

select pp.us_cong_district_latest
,pp.state_house_district_latest 
,sum(scr.support) as dem_share_24
,sum(1-scr.support) as gop_share_24
,sum(scr22.dnc_2022_dem_party_support) as dem_share_22
,sum(1-scr22.dnc_2022_dem_party_support) as gop_share_22
,sum(case when pp.neighborhood_category = 'Rural' then (scr.support + scr22.dnc_2022_dem_party_support)/2 else scr.support end) as dem_share_combo
,sum(case when pp.neighborhood_category = 'Rural' then ((1-scr.support) + (1-scr22.dnc_2022_dem_party_support))/2 else (1-scr.support) end) as gop_share_combo



from current_status cs
  join democrats.analytics_wa.person pp
  on pp.sos_id = format('WA%09d',safe_cast(cs.voter_id as int))
    join democrats.scores_wa.current_scores scr
    using(person_id)
      join democrats.scores_wa.all_scores_2022 scr22
      using(person_id)

where pp.reg_voter_flag is true

group by 1,2