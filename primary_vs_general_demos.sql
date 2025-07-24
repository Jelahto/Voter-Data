select pp.us_cong_district_latest
,pp.media_market
,coalesce(pp.gender_combined,'U') as gender_combined
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
,case when ca.has_early_voted is true then 'Primary Voter' else 'Non Primary Voter' end as primary_vote
,sum(case when ca.myv_van_id is not null then 1 else 0 end) as primary_ballots
,sum(case when ca.myv_van_id is not null then greatest(sc.clarity_2022_turnout,.92) else coalesce(sc.clarity_2022_turnout,.15) end ) as proj_general_votes


from democrats.analytics_wa.person pp
    join democrats.scores_wa.all_scores_2022 sc 
    on sc.person_id = pp.person_id
      left join vansync.contacts_absentees ca
      on ca.myv_van_id = pp.myv_van_id

where pp.reg_voter_flag is true

group by 1,2,3,4,5,6
