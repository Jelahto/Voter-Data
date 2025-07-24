with base as
(select pp.county_name
,van_precinct_name
,avg(voting_address_latitude) as precinct_lat
,avg(voting_address_longitude) as precinct_long
,count(*) as reg_voters
,sum(case when mb.Voter_ID is not null then 1 else 0 end) as ballots_returned


from democrats.analytics_wa.person pp
  left join democrats.av_ev_wa_20241105_general.WA_statewide_18_source mb
  on pp.sos_id = format('WA%09d',safe_cast(mb.voter_id as int))

where pp.reg_voter_flag is true
and (pp.reg_status_id = '1' or mb.voter_id is not null)
and pp.county_name = 'King'
and van_precinct_name is not null

group by 1,2
)

select b.*
,st_simplify(geo.geom,1) as geom

from base b
  join districts.2022_precinct_geoms geo
    on geo.county_name = b.county_name 
    and trim(upper(geo.precinct_name)) = trim(upper(b.van_precinct_name))