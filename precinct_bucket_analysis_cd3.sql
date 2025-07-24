with returns as(
  select *
  from democrats.av_ev_wa_20220802_primary.WA_statewide_47_source

  UNION ALL 

  select *
  from democrats.av_ev_wa_20220802_primary.WA_statewide_48_source

  UNION ALL 

  select *
  from democrats.av_ev_wa_20220802_primary.WA_statewide_46_source
)    

,projections as(
select cnc.countycode as county_code
,pp.county_name
,pp.van_precinct_name as precinct_name
,democrats.functions.width_bucket(scr.dnc_2022_dem_party_support,0,1,5) as support_bucket
,count(*) as ballots_cast
,sum(case when pp.gender_combined = 'F' then 1 else 0 end) as women_voters
,sum(case when pp.ethnicity_combined = 'W' then 1 else 0 end) as white_voters
,avg(cast(right(pp.voting_address_urbanicity,1) as int)) as urbanicity
,avg(pp.age_combined) as avg_age


from democrats.analytics_wa.person pp 
  join commons.county_name_to_code cnc
  on cnc.countyname = pp.county_name 
    join democrats.scores_wa.all_scores_2022 scr
    using(person_id)
      join returns r
      on pp.sos_id = format('WA%09d',cast(voter_id as int)) 
      and pp.reg_voter_flag is true 
      and r.challenge_reason is null 

group by 1,2,3,4
)

,candidate_votes as(
select ppr.county_code
,ppr.precinct_name
,case when candidate = 'Marie Gluesenkamp Perez' then votes end as mgp_votes
,case when candidate = 'Jaime Herrera Beutler' then votes end as jhb_votes
,case when candidate = 'Joe Kent' then votes end as jk_votes
,case when candidate in ('Chris Byrd','Vicki Kraft','Oliver Black','Heidi St. John','Davy Ray','Leslie L. French','WRITE-IN') then votes end as other_votes

from election_results.2022_primary_precinct_results ppr
  join election_results.22_primary_congressional_candidate_party scp
  using(candidate)

where race = 'CONGRESSIONAL DISTRICT 3 - U.S. Representative'
)

,precinct_vote_totals as(
select county_code
,precinct_name
,max(mgp_votes) as mgp_votes
,max(jhb_votes) as jhb_votes
,max(jk_votes) as jk_votes
,sum(other_votes) as other_votes

from candidate_votes 

group by 1,2)
, buckets as(
select p.county_name
,p.county_code
,p.precinct_name 
,case when p.support_bucket = 0 then (ballots_cast) end as voters_0_to_20
,case when p.support_bucket = 1 then (ballots_cast) end as voters_20_to_40
,case when p.support_bucket = 2 then (ballots_cast) end as voters_40_to_60
,case when p.support_bucket = 3 then (ballots_cast) end as voters_60_to_80
,case when p.support_bucket = 4 then (ballots_cast) end as voters_80_to_100


from projections p 
)
,precinct_buckets as (
select county_name
,county_code 
,precinct_name
,max(voters_0_to_20) as voters_0_to_20
,max(voters_20_to_40) as voters_20_to_40
,max(voters_40_to_60) as voters_40_to_60
,max(voters_60_to_80) as voters_60_to_80
,max(voters_80_to_100) as voters_80_to_100

from buckets

group by 1,2,3
)
select p.county_name
,p.precinct_name
,pvt.mgp_votes/(mgp_votes + jhb_votes + jk_votes + other_votes) as mgp_share
,pvt.jhb_votes/(mgp_votes + jhb_votes + jk_votes + other_votes) as jhb_share
,pvt.jk_votes/(mgp_votes + jhb_votes + jk_votes + other_votes) as jk_share
,voters_0_to_20/(voters_0_to_20 + voters_20_to_40 + voters_40_to_60 + voters_60_to_80 + voters_80_to_100) as share_0_to_20
,voters_20_to_40/(voters_0_to_20 + voters_20_to_40 + voters_40_to_60 + voters_60_to_80 + voters_80_to_100) as share_20_to_40
,voters_40_to_60/(voters_0_to_20 + voters_20_to_40 + voters_40_to_60 + voters_60_to_80 + voters_80_to_100) as share_40_to_60
,voters_60_to_80/(voters_0_to_20 + voters_20_to_40 + voters_40_to_60 + voters_60_to_80 + voters_80_to_100) as share_60_to_80
,voters_80_to_100/(voters_0_to_20 + voters_20_to_40 + voters_40_to_60 + voters_60_to_80 + voters_80_to_100) as share_80_to_100

from precinct_buckets p
  join precinct_vote_totals pvt
  on p.county_code = pvt.county_code
  and trim(upper(p.precinct_name)) = trim(upper(pvt.precinct_name))

where p.county_code != 'OK'
