
with targets as 
(select u.myc_van_id 
,initcap(pmc.first_name) as first_name
,initcap(pmc.middle_name) as middle_name
,initcap(pmc.last_name) as last_name
,coalesce(cpmc.phone_number,pp.primary_phone_number) as preferred_phone
,pp.voting_address_longitude 
,pp.voting_address_latitude
,row_number() over(partition by pp.voting_address_id order by farm_fingerprint(pp.person_id)) as address_rank -- 1 person per household
,ar.fo_name

from commons.vol_tiers_today u -- list of targets from MyC
  join vansync.person_records_myc pmc
  using(myc_van_id)
    join democrats.analytics_wa.person pp --could be a left join if we had a good way to generate a geocode for people not in myv
    on pp.myv_van_id = pmc.myv_van_id 
    and pp.reg_voter_flag is true
      left join vansync.contacts_phones_myc cpmc
      on pmc.phone_id = cpmc.contacts_phone_id
      join demswasp.vansync.activity_regions ar
      on ar.myc_van_id = u.myc_van_id

where (pp.primary_phone_number is not null or cpmc.phone_number is not null)
and u.target_subgroup_name in ('New Leads','Prospects Tier A','Prospects Tier B','Prospects Tier C','Prospects Tier D','Prospects Tier E','Prospects Tier F' )
)

, dvc_asks as 
(select t.myc_van_id
,t.first_name
,middle_name 
,t.last_name
,t.preferred_phone 
,voting_address_longitude 
,voting_address_latitude
,e.type
,concat(format_date('%A', date(datetime_offset_begin,'America/Los_Angeles')), ', ', -- Day of the week
            format_date('%B %e', date(datetime_offset_begin,'America/Los_Angeles')), -- Month and day
            CASE 
                WHEN EXTRACT(DAY FROM date(datetime_offset_begin,'America/Los_Angeles')) IN (1, 21, 31) THEN 'st'
                WHEN EXTRACT(DAY FROM date(datetime_offset_begin,'America/Los_Angeles')) IN (2, 22) THEN 'nd'
                WHEN EXTRACT(DAY FROM date(datetime_offset_begin,'America/Los_Angeles')) IN (3, 23) THEN 'rd'
                ELSE 'th' 
            END
        ) as event_date -- Formats to something like 'Saturday, September 14th'
,e.shift_start_time
,e.event_name
,coalesce(l.location_name, 'Via Zoom') as location_name
,CONCAT(coalesce(street_num,''),' ',coalesce(street_num_half,''),' ',coalesce(street_prefix,''),' ',coalesce(street_name,''),' ',coalesce(street_type,''),' ',coalesce(street_suffix,'')) as event_address
,coalesce(l.city,'Statewide') as event_city
,st_distance(st_geogpoint(cast(l.longitude as float64),cast(l.latitude as float64)),st_geogpoint(t.voting_address_longitude,t.voting_address_latitude))/1609 as distance_miles

from targets t
  join demswasp.commons.recruitment_dialers e
  on t.fo_name = e.turf
    left join demswasp.vansync.events_locations el
    on el.event_id = e.event_id
      left join demswasp.vansync.locations l
      on l.location_id = el.location_id
        join vansync.events ev
        on ev.event_id = e.event_id

where t.address_rank = 1 
  ) --list of events to invite to

, dvc_invites as(
select myc_van_id
,first_name
,middle_name 
,last_name 
,voting_address_longitude 
,voting_address_latitude
,preferred_phone 
,max(case when type= 'Canvass' then event_name  else null end) as canvass_event_name
,max(case when type= 'Phonebank'then event_name else null end) as phone_bank_event_name
,max(case when type= 'Canvass' then event_date else null end) as canvass_date
,max(case when type= 'Phonebank'then event_date else null end) as phonebank_date
,max(case when type= 'Canvass' then shift_start_time else null end) as canvass_shift_start_time
,max(case when type= 'Phonebank'then shift_start_time else null end) as phonebank_shift_start_time
,max(case when type= 'Canvass' then location_name  else null end) as canvass_location_name
,max(case when type= 'Phonebank'then location_name else null end) as phonebank_location_name
,max(case when type= 'Canvass' then event_address  else null end) as canvass_address
,max(case when type= 'Phonebank'then event_address else null end) as phonebank_address
,max(case when type= 'Canvass' then event_city  else null end) as canvass_city
,max(case when type= 'Phonebank'then event_city else null end) as phonebank_city

from dvc_asks

where distance_miles <= 30
or distance_miles is null

group by 1,2,3,4,5,6,7
)
,tour_cross_list as(
select *
,st_distance(st_geogpoint(a.voting_address_longitude,a.voting_address_latitude),st_geogpoint(b.venue_long,b.venue_lat))/1609 as tour_distance
,row_number() over(
    partition by a.myc_van_id 
    order by st_distance(st_geogpoint(a.voting_address_longitude,a.voting_address_latitude),st_geogpoint(b.venue_long,b.venue_lat)) asc nulls last
) tour_dist_rank

from dvc_invites a
CROSS JOIN 
commons.gotv_tour_dialer_asks b
)

select *

from tour_cross_list

where tour_dist_rank = 1
and tour_distance <= 30


