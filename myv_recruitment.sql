--added 1 per address to avoid double calls on the new dialer
--insert into traffic_control.checkout

with base as 
(select pp.myv_van_id
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
  ,FARM_FINGERPRINT(pp.myv_van_id)
) as phone_rank
--*/
,pp.us_cong_district_latest as cd
,pp.state_house_district_latest as ld
,pp.county_name
,t.fo_name
,cs.volunteer_propensity
,pp.voting_address_longitude as long
,pp.voting_address_latitude as lat
,row_number() over(
  partition by pp.voting_address_id 
  order by 
  cs.volunteer_propensity desc 
  ,FARM_FINGERPRINT(pp.myv_van_id) desc 
) as address_order 


from democrats.analytics_wa.person pp
  left join traffic_control.checkout tc
  on tc.phone = pp.primary_cell_number -- use pp.primary_phone_number for dialer lists, pp.primary_cell_number for text lists
  and cast(tc.checkout_expiration as date) >= current_date("America/Los_Angeles")  
    left join traffic_control.checkout tc2
    on tc2.voter_id = pp.myv_van_id
    and cast(tc.checkout_expiration as date) >= current_date("America/Los_Angeles")
      left join vansync.person_records_myc pmc 
      on pmc.myv_van_id = pp.myv_van_id 
      and pmc.person_committee_id = '112164'
        join vansync.turf t
        on t.van_precinct_id = pp.van_precinct_id
        and t.committee_id = '107800'
          join democrats.scores_wa.current_scores cs
          on cs.person_id = pp.person_id


where tc.phone is null 
and tc2.voter_id is null
and pp.primary_cell_number is not null --cell for text phone for dialer
and pmc.myv_van_id is null 
and pp.reg_voter_flag is true 
and pp.reg_status_id = '1'
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
,fo_name 
,long
,lat
,row_number()over(
  partition by fo_name --comment out for non-organizer targeting
  order by volunteer_propensity desc
  ,farm_fingerprint(myv_van_id)
) as vol_prop_rank


from base b 
  left join attempted a
  using(myv_van_id)

where phone_rank = 1
and address_order = 1
and a.myv_van_id is null

)
--/*
,events as(

select rd.turf
,coalesce(rd.special_description,rd.type) as description
,rd.event_name
,rd.event_id
,rd.shift_start_time
,date
,l.latitude
,l.longitude
,location_name
,concat(street_num, ifnull(concat(' ',street_num_half),''), ifnull(concat(' ',street_prefix),''),ifnull(concat(' ',street_name),''), ifnull(concat(' ',street_type),''), ifnull(concat(' ',street_suffix),'')) as event_address
,city 


from commons.recruitment_dialers rd
  join vansync.events_locations el
  on el.event_id = rd.event_id 
    join vansync.locations l
    on el.location_id = l.location_id 


)

, distance_order as (
select myv_van_id
,first_name 
,last_name 
,phone
,long
,lat
,cd
,ld
,fo_name
,vol_prop_rank
,description
,date as event_date
,shift_start_time as event_times
,location_name
,event_address
,city
,st_distance(st_geogpoint((long),(lat)),st_geogpoint(cast(longitude as FLOAT64),cast(latitude as FLOAT64)))/1609 as distance_miles
,row_number() over(
  partition by location_name, description
  order by st_distance(st_geogpoint((long),(lat)),st_geogpoint(cast(longitude as FLOAT64),cast(latitude as FLOAT64)))/1609 asc 
) as dist_rank_event
,row_number() over(
  partition by myv_van_id, description
  order by st_distance(st_geogpoint((long),(lat)),st_geogpoint(cast(longitude as FLOAT64),cast(latitude as FLOAT64)))/1609 asc 
) as best_event

from check
  cross join events

where check.fo_name = events.turf
)
-----hubdialer upload
--/*
select myv_van_id
,first_name
,last_name 
,phone 
,description
,event_date
,event_times
,location_name
,concat(event_address," ",city) as event_location

from distance_order 

where vol_prop_rank <= 200
and distance_miles <= 20
and best_event = 1
--*/

------ Produces meta data for checkout tables
/*
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
--*/

--select *

--from distance_order

--limit 10
