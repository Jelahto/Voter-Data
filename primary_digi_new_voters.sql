select pp.myv_van_id 
,first_name
,middle_name
,last_name
,voting_street_address as address
,voting_city as city
,voting_zip as zip
,pp.primary_landline_number as landline
,pp.primary_cell_number as cell
,pp.date_of_birth_combined as date_of_birth
,pp.age_combined as age


from democrats.analytics_wa.person pp
  join democrats.scores_wa.current_scores cs
  using(person_id)
    join democrats.analytics_wa.person_votes pv
    using(person_id)

where pp.reg_voter_flag is true 
and cs.support > .5
and pp.earliest_reg_date >= '2020-11-01'
and pv.vote_g_2020 + pv.vote_p_2022 + pv.vote_g_2022 = 0
