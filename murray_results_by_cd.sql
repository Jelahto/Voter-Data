with party_votes as(
select distinct ppr.county_code
,ppr.precinct_name
,ppr.candidate
,votes

from election_results.2022_primary_precinct_results ppr
  join election_results.22_primary_senate_candidate_party scp
  using(candidate)

where race = 'U.S. Senator'

)
,cd_precincts_dupe as(

select us_cong_district_latest
,county_name
,van_precinct_name
,row_number()over(
  partition by county_name, van_precinct_name 
  order by count(*) desc
) as precinct_rank

from democrats.analytics_wa.person 

where reg_voter_flag is true

group by 1,2,3
)
,cd_precincts as(
select *

from cd_precincts_dupe 

where precinct_rank = 1
)


select pp.us_cong_district_latest
,pv.candidate
,sum(votes)

from cd_precincts pp
  join `demswasp.commons.county_name_to_code` cntc
  on pp.county_name = cntc.countyname 
    join party_votes pv
    on pv.county_code = cntc.countycode
    and trim(upper(pv.precinct_name)) = trim(upper(pp.van_precinct_name))

group by 1,2
order by 1