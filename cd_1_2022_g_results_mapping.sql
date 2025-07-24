create table election_results.cd_1_2022_g_results_mapping as

select rs.st_code 
,geo.county_name 
,geo.voting_district_name
,rs.G22C01DELB as dem_votes
,rs.G22C01CAVA as gop_votes
,geo.geom 


from election_results.2022_general_precinct_results_full rs
  join commons.county_name_to_code cnc
  on left(rs.st_code,2) = cnc.countycode
--    join districts.county_fips cf
--    on cf.county = cnc.countyname
    left join democrats.reference_geo.voting_tabulation_districts geo
    on geo.county_name = cnc.countyname
    and right(rs.st_code,4) = right(geo.voting_district_id,4)
    and geo.state_code = 'WA'

where rs.G22C01DELB is not null

union all 

select 'hold' 
,'King'
,'Ignore this Reference Data'
,0
,338
,ST_GEOGPOINT(-122.3289101,47.5539127)