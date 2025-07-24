create OR REPLACE table reporting.matchback_today_p24 as
with current_status as (
    select distinct *
from
(select *

from democrats.av_ev_wa_20240806_primary.WA_statewide_2_source --redirect to most recent file after upload is processed


) )
--,check as(
select coalesce(pp.gender_combined,'U') as gender_combined
,CASE WHEN pp.age_combined <= 29 THEN '18-29'
  WHEN pp.age_combined BETWEEN 30 AND 44 THEN '30-44'
  WHEN pp.age_combined BETWEEN 45 AND 64 THEN '45-64'
  WHEN pp.age_combined >= 65 THEN '65+'
  ELSE 'Not Specified' END AS age
,case when pp.ethnicity_combined = 'H' then 'Hispanic/Latino'
      when pp.ethnicity_combined = 'B' then 'African American'
      when pp.ethnicity_combined = 'W' then 'Caucasian'
      when pp.ethnicity_combined = 'A' then 'Asian American'
      when pp.ethnicity_combined = 'N' then 'Native American'
      else 'Unknown' end as ethnicity 
,concat(democrats.functions.width_bucket(least(scr.support, .99),0,1,5)*20, ' - ',democrats.functions.width_bucket (least(scr.support, .99),0,1,5)*20+20) as support_bucket
,coalesce(concat(democrats.functions.width_bucket(scr.turnout,0,1,5)*20,' - ',democrats.functions.width_bucket(scr.turnout,0,1,5)*20+20),' Unscored') as turnout_bucket


,cs.ballot_status
,case when strpos(received_date,' ') = 9 then cast(concat(right(left(received_date,8),4),'-0',left(received_date,1),'-0',right(left(received_date,3),1)) as date)
      when strpos(received_date,' ') = 10 and strpos(received_date,'/') = 2 then safe_cast(concat(right(left(received_date,9),4),'-0',left(received_date,1),'-',right(left(received_date,4),2)) as date)
      when strpos(received_date,' ') = 10 and strpos(received_date,'/') = 3 then safe_cast(concat(right(left(received_date,9),4),'-',left(received_date,2),'-0',right(left(received_date,4),1)) as date)
    else null end as received_date
,initcap(cs.challenge_reason) as challenge_reason
,pp.county_name
,pp.us_cong_district_latest as cd 
,pp.state_house_district_latest as ld
,concat(pp.state_fips,pp.county_fips) as county_fips_long
, sum(case when cs.voter_id is not null then 1 else 0 end) as ballot_count
, sum(case when cs.ballot_status in ('Accepted','Received') then 1 else 0 end) as votes_cast
, count(*) as registered_voters

from democrats.analytics_wa.person pp
  left join current_status cs
  on pp.sos_id = format('WA%09d',safe_cast(cs.voter_id as int))
    join democrats.scores_wa.current_scores scr
    on scr.person_id = pp.person_id



where pp.reg_voter_flag is true
and scr.support is not null
and pp.us_cong_district_latest is not null
and pp.state_house_district_latest is not null
and (pp.reg_voterfile_status = 'Active' or cs.voter_id is not null)

group by 1,2,3,4,5,6,7,8,9,10,11,12
/*)

select sum(votes_cast)

FROM check

where ballot_status is not null
and received_date is null*/