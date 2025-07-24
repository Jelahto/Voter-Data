create or replace table geom_projects.university_map as

with wa_cds as 
(select us_cong_district_name
,geom

from democrats.reference_geo.us_cong_districts

where state_code = 'WA'
)
,uni as(
  select school_name 
,address
,priority
,website
,institution_type 
,enrollment_size
,welcome_week
,coalesce(contact_name,"") as contact_name
,coalesce(contact_email,"") as contact_email
,coalesce(contact_phone,"") as contact_phone
,st_geogpoint(long,lat) as geo_point

from commons.college_campuses c

where c.ignore is false
)
,uni_x_cd as(
select *
,st_within(m.geo_point,d.geom) as cd_test

from uni m

cross join wa_cds d 
)

select *

from uni_x_cd

where cd_test is true