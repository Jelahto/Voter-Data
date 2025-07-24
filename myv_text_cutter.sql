--insert into traffic_control.checkout

with base as 
(select b.myv_van_id
,initcap(pp.first_name) as first_name
,initcap(pp.last_name) as last_name
/* --Lands and Cells
,pp.primary_phone_number as phone
,row_number()over(
  partition by pp.primary_phone_number
  order by case when pp.primary_phone_connection = 'L' then pp.primary_landline_quality_score
                when pp.primary_phone_connection = 'C' then pp.primary_cell_quality_score end DESC
                ,FARM_FINGERPRINT(b.myv_van_id)
) as phone_rank
--*/
--/* -- Cells only
,pp.primary_cell_number as phone 
,row_number()over(
  partition by pp.primary_cell_number 
  order by pp.primary_cell_quality_score desc
  ,FARM_FINGERPRINT(b.myv_van_id) desc
) as phone_rank
--*/
,pp.us_cong_district_latest as cd
,pp.state_house_district_latest as ld
,pp.county_name
,db.polling_location as db_name
,db.location_description as db_notes
,db.address as db_address
,db.city as db_city
,scr.dnc_2022_gotv 
,target_subgroup_name 


from commons.dvc_targets_today b
  join democrats.analytics_wa.person pp
  on b.myv_van_id = pp.myv_van_id 
  and pp.reg_voter_flag is true 
    join commons.2022_best_dropbox_by_precinct db
    on db.van_precinct_id = pp.van_precinct_id
      left join traffic_control.checkout tc
      on tc.phone = pp.primary_cell_number -- use pp.primary_phone_number for dialer lists, pp.primary_cell_number for text lists
      and cast(tc.checkout_expiration as date) >= current_date("America/Los_Angeles")  
        left join traffic_control.checkout tc2
        on tc2.voter_id = pp.myv_van_id
        and cast(tc.checkout_expiration as date) >= current_date("America/Los_Angeles")
          join democrats.scores_wa.all_scores_2022 scr 
          on pp.person_id = scr.person_id 

where tc.phone is null 
and tc2.voter_id is null
and pp.reg_voterfile_status = 'Active'
and pp.primary_cell_number is not null --cell for text phone for dialer
and (b.target_subgroup_name in ('Mobilization Dems','Prio Base Dems','Base Dems') or ((state_house_district_latest in ('005','010','024','026','029','030','038','042','044','047') or us_cong_district_latest in ('003','008')) and b.target_subgroup_name in ('Core Dems')))


)
,attempted as(
select myv_van_id
,sum(case when contact_type_name != 'No Actual Contact' then 1 else 0 end) as attempts

from vansync.contacts_contacts_myv ccmv
  join democrats.vansync_ref.contact_types ct
  using (contact_type_id)

where date(datetime_canvassed,"America/Los_Angeles") >= '2022-08-10'
and committee_id in ('59691','102353')
and contact_type_name != 'No Actual Contact'
group by 1
)
, check as (
select b.myv_van_id
,first_name 
,last_name 
,phone
,cd
,b.ld
,county_name
,db_name
,db_notes
,db_address
,db_city
,case when cd = '009' then 'Representative Adam Smith'
      when cd = '008' then 'Doctor Kim Schrier'
      when cd = '007' then 'Representative Pramila Jayapal'
      when cd = '006' then 'Representative Derek Kilmer'
      when cd = '005' then 'Natasha Hill'
      when cd = '004' then 'Doug White'
      when cd = '003' then 'Marie Perez'
      when cd = '002' then 'Representative Rick Larsen'
      when cd = '001' then 'Representative Suzan DelBene'
      when cd = '010' then 'Representative Marilyn Strickland'
      else null end as cong_cand
,case when sl.senate_candidate is not null and sl.house_1_candidate is not null and sl.house_2_candidate is not null
        then concat('Your State Senate candidate is ',sl.senate_candidate,' and your State House candidates are ',sl.house_1_candidate,' and ',sl.house_2_candidate,'.')
      when sl.senate_candidate is not null and (sl.house_1_candidate is not null or sl.house_2_candidate is not null)
        then concat('Your State Senate candidate is ',sl.senate_candidate,' and your State House candidate is ',coalesce(sl.house_1_candidate,sl.house_2_candidate),'.')
      when sl.senate_candidate is not null and sl.house_1_candidate is null and sl.house_2_candidate is null
        then concat('Your State Senate candidate is ',sl.senate_candidate,'.')
      when sl.senate_candidate is null and sl.house_1_candidate is not null and sl.house_2_candidate is not null
        then concat('Your State House candidates are ',sl.house_1_candidate,' and ',sl.house_2_candidate,'.')
      when sl.senate_candidate is null and (sl.house_1_candidate is not null or sl.house_2_candidate is not null)
        then concat('Your State House candidate is ',coalesce(sl.house_1_candidate,sl.house_2_candidate),'.')
      else null end as leg_string
,row_number()over(
  partition by cd --comment out for non-CD targeting
  order by 
  attempts asc nulls first
  ,dnc_2022_gotv desc
  ,farm_fingerprint(myv_van_id)
) as sbt
,attempts 
,target_subgroup_name


from base b 
  join commons.22_general_slate sl
  on b.ld = sl.state_house_district_latest
    left join attempted a
    using(myv_van_id)

where phone_rank = 1
and (a.attempts is null or(a.attempts<2))

)
--/*
--,maybe as (
select myv_van_id
,first_name 
,last_name 
,phone
,cong_cand
,leg_string
,db_name
,db_notes
,db_address
,db_city
--,cd
--,target_subgroup_name
--,ld
--,county_name

from check


where --(attempts is null or
 ((cd in ('001','008') and sbt <= 2430*1)
or (cd = '002' and sbt <= 2503*1)
or (cd = '003' and sbt <= 2284*1)
or (cd = '004' and sbt <= 680*1)
or (cd = '005' and sbt <= 972*1)
or (cd = '006' and sbt <= 2187*1)
or (cd in ('007','009') and sbt <= 2236*1)
or (cd = '010' and sbt <= 2041*1) )--)

--*/
/*
select myv_van_id
,'myv_van_id'
,phone
,'Nov 7 Statewide Mob and Prio Base finish 2nd pass G1'
,'2022-11-07'
,'2022-11-07'
,'texts'

from check
/*
where (cd in ('001','008') and sbt <= 2430*1.5)
or (cd = '002' and sbt <= 2503*2)
or (cd = '003' and sbt <= 2284*1)
or (cd = '004' and sbt <= 680*1)
or (cd = '005' and sbt <= 972*1)
or (cd = '006' and sbt <= 2187*1.75)
or (cd in ('007','009') and sbt <= 2236*1.5)
or (cd = '010' and sbt <= 2041*1.5) 

--*/
/*
select cd,target_subgroup_name, count(*)

from maybe

group by 1,2
order by 1,2
--*/