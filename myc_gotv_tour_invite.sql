with targets as 
(select u.myc_van_id 
,initcap(pmc.first_name) as first_name
,initcap(pmc.middle_name) as middle_name
,initcap(pmc.last_name) as last_name
,coalesce(cpmc.phone_number,pp.primary_phone_number) as preferred_phone
,pp.voting_address_longitude 
,pp.voting_address_latitude
,row_number() over(partition by pp.voting_address_id order by farm_fingerprint(pp.person_id)) as address_rank -- 1 person per household

from commons.vol_tiers_today u -- list of targets from MyC
  join vansync.person_records_myc pmc
  using(myc_van_id)
    join democrats.analytics_wa.person pp --could be a left join if we had a good way to generate a geocode for people not in myv
    on pp.myv_van_id = pmc.myv_van_id 
    and pp.reg_voter_flag is true
      left join vansync.contacts_phones_myc cpmc
      on pmc.phone_id = cpmc.contacts_phone_id

where (pp.primary_phone_number is not null or cpmc.phone_number is not null)
and u.target_subgroup_name in ('New Leads','Tier B','Tier C','Tier D','Tier E','Tier F' )
)

, events_cross_targets as 
(select t.myc_van_id
,t.first_name
,middle_name 
,t.last_name
,t.preferred_phone 
,e.event_description
,e.start_date 
,e.event_start 
,e.location_name 
,e.street_address as event_address
,e.city as event_city
,st_distance(st_geogpoint(cast(e.longitude as float64),cast(e.latitude as float64)),st_geogpoint(t.voting_address_longitude,t.voting_address_latitude))/1609 as distance_miles

from targets t
  cross join commons.primary_gotvtour_events e
  
where t.address_rank = 1 
  ) --list of events to invite to
,dist_rank as
(select *
,row_number() over(
  partition by myc_van_id
  order by distance_miles asc nulls last
) as distance_rank

from events_cross_targets)

select *

from dist_rank 

where distance_rank = 1
and ((distance_miles <= 25 and event_city in ('Kirkland','Seattle') or (event_city not in ('Kirkland','Seattle') and distance_miles <= 50)))