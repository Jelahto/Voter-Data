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
,sum(scr.dnc_2022_dem_party_support) as proj_dem_votes
,sum(1-scr.dnc_2022_dem_party_support) as proj_gop_votes
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

group by 1,2,3
)

,party_votes as(
select ppr.county_code
,ppr.precinct_name
,scp.party_code
,sum(votes) as party_votes

from election_results.2022_primary_precinct_results ppr
  join election_results.22_primary_sos_candidate_party scp
  using(candidate)

where ppr.race in ('Secretary of State','State of Washington Secretary of State')

group by 1,2,3)

, cast_1 as(
select county_code
,precinct_name
,case when party_code = 'd' then p.party_votes else null end as actual_dem_votes
,case when party_code = 'r' then p.party_votes else null end as actual_gop_votes
,case when party_code = 'i' then p.party_votes else null end as actual_ind_votes

from party_votes p)
,precinct_vote_totals as(
select county_code
,precinct_name
,max(actual_dem_votes) as actual_dem_votes
,max(actual_gop_votes) as actual_gop_votes
,max(actual_ind_votes) as actual_ind_votes  

from cast_1 

group by 1,2)

select p.county_name
,p.precinct_name 
,p.proj_dem_votes 
,p.proj_gop_votes 
,pvt.actual_dem_votes
,pvt.actual_gop_votes
,pvt.actual_ind_votes
,p.ballots_cast
,p.women_voters
,p.white_voters
,p.urbanicity 
,p.avg_age


from projections p
  join precinct_vote_totals pvt
  on p.county_code = pvt.county_code
  and trim(upper(p.precinct_name)) = trim(upper(pvt.precinct_name))

where p.county_code != 'OK'
