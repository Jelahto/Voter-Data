--added 1 per address to avoid double calls on the new dialer
--insert into traffic_control.checkout

with base as 
(select a.myv_van_id
,initcap(pp.first_name) as first_name
,initcap(pp.last_name) as last_name
--/* --Lands and Cells
,pp.primary_phone_number as phone
,row_number()over(
  partition by pp.primary_phone_number
  order by case when pp.primary_phone_connection = 'L' then pp.primary_landline_quality_score
                when pp.primary_phone_connection = 'C' then pp.primary_cell_quality_score end DESC
                ,FARM_FINGERPRINT(b.myv_van_id)
) as phone_rank
--*/
/* -- Cells only
,pp.primary_cell_number as phone 
,row_number()over(
  partition by pp.primary_cell_number 
  order by pp.primary_cell_quality_score desc
  ,FARM_FINGERPRINT(b.myv_van_id)
) as phone_rank
,db.polling_location as db_name
,db.location_description as db_notes
,db.address as db_address
,db.city as db_city 
--*/
,pp.us_cong_district_latest as cd
,pp.state_house_district_latest as ld
,pp.county_name
,combo_support
,turnout
,pp.voting_address_longitude as long
,pp.voting_address_latitude as lat
,row_number() over(
  partition by pp.voting_address_id 
  order by 
  combo_support desc 
  ,FARM_FINGERPRINT(b.myv_van_id) desc 
) as address_order 


from commons.dvc_targets_today b
  join commons.dvc_v2_base a
  on a.myv_van_id = b.myv_van_id
  join democrats.analytics_wa.person pp
  on a.myv_van_id = pp.myv_van_id 
  and pp.reg_voter_flag is true 
    ----join commons.2022_best_dropbox_by_precinct db
    ----on db.van_precinct_id = pp.van_precinct_id
      left join traffic_control.checkout tc
      on tc.phone = pp.primary_phone_number -- use pp.primary_phone_number for dialer lists, pp.primary_cell_number for text lists
      and cast(tc.checkout_expiration as date) >= current_date("America/Los_Angeles")  
        left join traffic_control.checkout tc2
        on tc2.voter_id = pp.myv_van_id
        and cast(tc.checkout_expiration as date) >= current_date("America/Los_Angeles")
          left join vansync.person_records_myc pmc 
          on pmc.myv_van_id = pp.myv_van_id 
          and pmc.person_committee_id = '112164'

where tc.phone is null 
and tc2.voter_id is null
and pp.primary_phone_number is not null --cell for text phone for dialer
and pmc.myv_van_id is null 
and b.target_subgroup_name in ('Core Dems')
--and pp.age_combined <= 45 --narrowing to older voters for calls or younger for texts
--and pp.voting_address_urbanicity in ('R1','R2','S3') There is a new measure -  on phoenix person pls update!
--and pp.us_cong_district_latest in ('007','009','001') 
)
,attempted as(
select distinct myv_van_id

from vansync.contacts_contacts_myv ccmv
  join democrats.vansync_ref.contact_types ct
  using (contact_type_id)

where date(datetime_canvassed,"America/Los_Angeles") >= '2024-06-01'
and committee_id = '112164'
and contact_type_name != 'No Actual Contact'
)
, check as (
select b.myv_van_id
,first_name 
,last_name 
,phone
,cd
,b.ld
--,db_name
--,db_notes
--,db_address
--,db_city
,long
,lat
,case when cd = '009' then 'Representative Adam Smith for Congress'
      when cd = '008' then 'Doctor Kim Schrier for Congress'
      when cd = '007' then 'Representative Pramila Jayapal for Congress'
      when cd = '006' then 'Chris Reykdal for superintendent of public instruction'
      when cd = '005' then 'Chris Reykdal for superintendent of public instruction'
      when cd = '004' then 'Chris Reykdal for superintendent of public instruction'
      when cd = '003' then 'Representative Marie Perez for Congress'
      when cd = '002' then 'Representative Rick Larsen for Congress'
      when cd = '001' then 'Representative Suzan DelBene for Congress'
      when cd = '010' then 'Representative Marilyn Strickland for Congress'
      else null end as custom_field_2
/*
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
      else null end as custom_field_1 --*/
,row_number()over(
  partition by cd --comment out for non-CD targeting
  order by combo_support*coalesce(turnout,.15) desc
  ,farm_fingerprint(myv_van_id)
) as sbt


from base b 
  join commons.22_general_slate sl
  on b.ld = sl.state_house_district_latest
    left join attempted a
    using(myv_van_id)

where phone_rank = 1
and address_order = 1
and a.myv_van_id is null

)
--/*
, distance_order as (
select myv_van_id
,first_name 
,last_name 
,phone
,long
,lat
--,db_name
--,db_notes
--,db_address
--,db_city
,cd
,ld
,event_description
,start_date as event_date
,start_time as event_time
,location_name
,street_address
,city
,st_distance(st_geogpoint((long),(lat)),st_geogpoint(cast(longitude as FLOAT64),cast(latitude as FLOAT64)))/1609 as distance_miles
,row_number() over(
  partition by location_name
  order by st_distance(st_geogpoint((long),(lat)),st_geogpoint(cast(longitude as FLOAT64),cast(latitude as FLOAT64)))/1609 asc 
) as dist_rank_event
,row_number() over(
  partition by myv_van_id 
  order by st_distance(st_geogpoint((long),(lat)),st_geogpoint(cast(longitude as FLOAT64),cast(latitude as FLOAT64)))/1609 asc 
) as best_event

from check
  cross join commons.primary_gotvtour_events
)
-----hubdialer upload
/*
select myv_van_id
,first_name
,last_name 
,phone 
,event_description
,event_date
,event_time
,location_name
,concat(street_address," ",city) as event_location

from distance_order 

where dist_rank_event <=1800
and best_event = 1
and distance_miles <= 20
--*/

------ Produces meta data for checkout tables
select myv_van_id
,'myv_van_id'
,phone
,'July 25 Core GOTV Tour Invite'
,'2024-07-25'--as checkout date
,'2024-07-26'--as checkout expiration
,'calls' -- contact type

from distance_order

where dist_rank_event <=1800
and best_event = 1
and distance_miles <= 20


--select *

--from distance_order

--limit 10
