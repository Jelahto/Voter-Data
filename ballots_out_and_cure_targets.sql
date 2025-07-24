with matchbacks as (

select voter_id, challenge_reason, ballot_status, received_date

from democrats.av_ev_wa_20241105_general.WA_statewide_35_source 

)

select pp.county_name 
,pp.us_cong_district_latest
,pp.state_house_district_latest 
,sum(case when mb.ballot_status != 'Rejected' then 1 else 0 end) as ballots_accepted
,sum(case when mb.challenge_reason in ('Signature Does Not Match','Unsigned') then 1 else 0 end ) as signature_challenges
,sum(case when mb.challenge_reason in ('Signature Does Not Match','Unsigned') then -1*(1-scr.support) else 0 end ) as gop_curable
,sum(case when mb.challenge_reason in ('Signature Does Not Match','Unsigned') then scr.support else 0 end ) as dem_curable

from matchbacks mb 
  join democrats.analytics_wa.person pp
  on pp.sos_id = format('WA%09d',cast(mb.voter_id as int))
  and pp.reg_voter_flag is true
    join democrats.scores_wa.current_scores scr
    using (person_id)

group by 1,2,3
