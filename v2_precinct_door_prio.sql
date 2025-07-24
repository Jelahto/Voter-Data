--create table sbx_andresb.three_target_walkable as 

with target_address as(
select distinct pp.voting_address_id 
,pp.voting_address_longitude as voting_long
,pp.voting_address_latitude as voting_lat

from commons.dvc_targets_today u 
  join democrats.analytics_wa.person pp 
  on u.myv_van_id = pp.myv_van_id 
  and pp.reg_voter_flag is true

where pp.voting_address_type = 'S'
--and u.target_subgroup_name in ('Prio Base Dems','Mobilization Dems','Soft Dems')
)
,walkable_address as(
select voting_address_id
,st_clusterdbscan(st_geogpoint(voting_long,voting_lat),500,30) over() as cluster_test

from target_address
),prio_address as(
select distinct pp.voting_address_id 
,pp.voting_address_longitude as voting_long
,pp.voting_address_latitude as voting_lat

from commons.dvc_targets_today u 
  join democrats.analytics_wa.person pp 
  on u.myv_van_id = pp.myv_van_id 
  and pp.reg_voter_flag is true

where pp.voting_address_type = 'S'
and u.target_subgroup_name in ('Prio Base Dems','Mobilization Dems')
)
,walkable_prio as(
select voting_address_id
,st_clusterdbscan(st_geogpoint(voting_long,voting_lat),500,30) over() as cluster_test

from prio_address
)

, base as (
select distinct pp.county_name 
,pp.van_precinct_name 
,count(distinct pp.voting_address_id) as walkable_targets
,null as walkable_prio


from democrats.analytics_wa.person pp
  join walkable_address wa
  on pp.voting_address_id = wa.voting_address_id
    join commons.dvc_v5_segments u
    on pp.person_id = u.person_id

where cluster_test is not null 

group by 1,2

UNION ALL 

select distinct pp.county_name 
,pp.van_precinct_name 
,null as walkable_targets
,count(distinct pp.voting_address_id) as walkable_prio


from democrats.analytics_wa.person pp
  join walkable_prio wa
  on pp.voting_address_id = wa.voting_address_id
    join commons.dvc_v5_segments u
    on pp.person_id = u.person_id

where cluster_test is not null 

group by 1,2
), attempts as
(select distinct myv_van_id

from vansync.contacts_contacts_myv ccmv

where committee_id in ('59691','102353')
and date(datetime_canvassed,"America/Los_Angeles") >= '2022-08-10'
and contact_type_id != '78')
, address_info as(
select t.region_name
,t.fo_name
,pp.county_name 
,pp.van_precinct_name 
,us_cong_district_latest
,pp.state_house_district_latest 
,pp.voting_address_id 
,sum(case when u.myv_van_id is not null then 1 else 0 end) as target_door
,sum(case when u.segment in ('Mobilization','Soft Dems','Prio Base') then 1 else 0 end) as prio_door
,sum(case when a.myv_van_id is not null then 1 else 0 end) as attempted

from democrats.analytics_wa.person pp
  left join commons.dvc_v4_segments u
  using(person_id)
    left join attempts a
    on a.myv_van_id = pp.myv_van_id 
    and pp.reg_voter_flag is true
      join vansync.turf t
      on pp.van_precinct_id = t.van_precinct_id
      and t.committee_id = '59691'
      

where pp.reg_voter_flag is true
and pp.is_deceased is false    
and county_name is not null
and van_precinct_name is not null 
and us_cong_district_latest is not null
and state_house_district_latest is not null

group by 1,2,3,4,5,6,7)
,base_dedupe as (
select county_name 
,van_precinct_name 
,max(walkable_targets) as walkable_targets
,max(walkable_prio) as walkable_prio

from base

group by 1,2

)
, check as(
select region_name as region
,fo_name as org_code
,county_name
,van_precinct_name
,us_cong_district_latest as CD
,state_house_district_latest as LD
,count(*) as total_doors
,coalesce(max(walkable_targets),0) as target_doors
,coalesce(max(walkable_prio),0) as prio_doors
,sum(case when attempted > 0 then 1 else 0 end) as post_primary_attempts

from address_info a
  left join base_dedupe b 
  using(county_name,van_precinct_name)

group by 1,2,3,4,5,6
)

select * 

from check 

where total_doors >= target_doors 