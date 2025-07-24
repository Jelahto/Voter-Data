create or replace table geom_projects.cd_ld_door_counts as

with map_base as
(select RIGHT(CONCAT('000', dcba.district),3) as ld
,RIGHT(CONCAT('000', cbcd.cd),3) as cd
,st_union_agg(cb.geom) as geo
,st_simplify(st_union_agg(cb.geom),1) as simple_geo

from democrats.reference_geo.census_blocks cb
  join demswasp.districts.daves_census_block_assignments dcba
  on cb.census_block = dcba.block_id
    join districts.daves_census_blocks_cd cbcd
    on cb.census_block = cbcd.block_id

where cb.state_code = 'WA'

group by 1,2)
, door_count as(
select coalesce(rup.new_ld, pp.state_house_district_latest) as ld
,pp.us_cong_district_latest as cd
,count(distinct pp.voting_address_id) as total_mob_doors

from democrats.analytics_wa.person pp
    join commons.dvc_v3_initial_list u
    using(person_id)
      left join districts.redistricting_updated_precincts_24 rup
      on pp.county_name = rup.county_name 
      and trim(upper(pp.van_precinct_name)) = trim(upper(rup.precinct_name))

where u.segment = 'Mobilization'

group by 1,2)

select *

from map_base mb
  join door_count dc
  using(ld,cd)