Non Primary Voters Base List

with base as (
  select 
      pp.state_house_district_latest,
      sum(scr.support) as dnc_24_dem_proj,
      sum(1 - scr.support) as dnc_24_gop_proj,
      sum(scr22.dnc_2022_dem_party_support) as dnc_22_dem_proj,
      sum(1 - scr22.dnc_2022_dem_party_support) as dnc_22_gop_proj,
      sum(case 
            when pp.neighborhood_category = 'Rural' 
            then (scr.support + scr22.dnc_2022_dem_party_support) / 2 
            else scr.support 
          end) as dnc_combo_dem_proj,
      sum(case 
            when pp.neighborhood_category = 'Rural' 
            then ((1 - scr.support) + (1 - scr22.dnc_2022_dem_party_support)) / 2 
            else (1 - scr.support) 
          end) as dnc_combo_gop_proj
  from 
      `democrats.scores_wa.current_scores` scr
  join 
      `democrats.analytics_wa.person` pp
  on 
      scr.person_id = pp.person_id
  join 
      `democrats.scores_wa.all_scores_2022` scr22
  on 
      scr.person_id = scr22.person_id
  join 
      `democrats.analytics_wa.person_votes` k
  on 
      pp.person_id = k.person_id
  where 
      pp.reg_voter_flag = true
      and pp.reg_status_id = '1'
      and (k.vote_p_2024 = 0)
  group by 
      pp.state_house_district_latest
)

select 
       state_house_district_latest,
       FORMAT('%0.2f', dnc_24_dem_proj) as dnc_24_dem_proj,
       FORMAT('%0.2f', dnc_24_gop_proj) as dnc_24_gop_proj,
       FORMAT('%0.2f', dnc_22_dem_proj) as dnc_22_dem_proj,
       FORMAT('%0.2f', dnc_22_gop_proj) as dnc_22_gop_proj,
       FORMAT('%0.2f', dnc_combo_dem_proj) as dnc_combo_dem_proj,
       FORMAT('%0.2f', dnc_combo_gop_proj) as dnc_combo_gop_proj,
       FORMAT('%0.2f', dnc_24_dem_proj - dnc_24_gop_proj) as proj_margin_24,
       FORMAT('%0.2f', dnc_22_dem_proj - dnc_22_gop_proj) as proj_margin_22
from base

order by state_house_district_latest;
