create table traffic_control.primary_digi_low_turnout_dems as
--/*
select pp.myv_van_id 
,pp.first_name
,pp.middle_name
,pp.last_name
,voting_street_address as address
,voting_city as city
,voting_zip as zip
,pp.primary_landline_number as landline
,pp.primary_cell_number as cell
,pp.date_of_birth_combined as date_of_birth
,pp.age_combined as age
--*/

--select count(*)

from democrats.analytics_wa.person pp
  join commons.dvc_v2_base u
  using(person_id)
    left join traffic_control.primary_digi_new_voters l
    on l.myv_van_id = pp.myv_van_id
  

where pp.reg_voter_flag is true 
and u.segment = 'Mobilization'
and l.myv_van_id is null