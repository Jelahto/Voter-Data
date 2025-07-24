/* primarily verify against Patty Murray results. Also check score performance in key races
CD 8
CD 3
LD 42
LD 26
LD 10

*/

with matchback as(
select *

from democrats.av_ev_wa_20220802_primary.WA_statewide_47_source

union all

select *

from democrats.av_ev_wa_20220802_primary.WA_statewide_48_source

union all 

select *

from democrats.av_ev_wa_20220802_primary.WA_statewide_46_source)

select 
pp.us_cong_district_latest
,pp.state_house_district_latest
,pp.van_precinct_name -- field name might be a little off
--,democrats.functions.width_bucket(scr.dnc_2022_dem_party_support,0,1,10) as support_bucket
--,democrats.functions.width_bucket(scr.clarity_2022_turnout,0,1,10) as turnout_bucket
--,sum(scr.dnc_2022_dem_party_support) - sum((1-scr.dnc_2022_dem_party_support)) as net_dem_votes
,sum(scr.dnc_2022_dem_party_support) as dem_votes 
,sum((1-scr.dnc_2022_dem_party_support)) as gop_votes

from democrats.analytics_wa.person pp
  join democrats.scores_wa.all_scores_2022 scr 
  using(person_id)
    left join matchback mb
    on pp.sos_id = format('WA%09d',cast(mb.voter_id as int))
    

where pp.reg_voter_flag is true
and mb.voter_id is not null
and mb.challenge_reason is null

group by 1,2,3--,4,5