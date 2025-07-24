create table election_results.cd_3_2022_p_results_mapping as

with results as 
(select --* --distinct race, candidate
pr.county_code
, pr.precinct_name
, pr.precinct_code
, gis.lookup_key
, sum(case when candidate = 'Marie Gluesenkamp Perez' then votes else 0 end) as mgp_votes
, sum(case when candidate = 'Joe Kent' then votes else 0 end) as kent_votes
, sum(case when candidate in ('Marie Gluesenkamp Perez','Davy Ray') then votes else 0 end) as dem_votes
, sum(case when candidate in ('Jaime Herrera Beutler','Vicki Kraft','Heidi St. John','Leslie L. French','Joe Kent') then votes else 0 end) as gop_votes


from election_results.2022_primary_precinct_results pr
  join `demswasp.commons.county_name_to_code` ctc
  on pr.county_code = ctc.countycode
    join districts.2022_precincts_gis_lookup gis 
    on gis.county_name = ctc.countyname
    and (trim(lower(pr.precinct_code)) = trim(lower(gis.precinct_id)) or trim(lower(pr.precinct_name)) = concat('0',trim(lower(gis.precinct_name))))

where race in ('CONGRESSIONAL DISTRICT 3 - U.S. Representative','Congressional District No. 3 United States Representative')
and pr.precinct_name not in ('Total', 'Countywide')

group by 1,2,3,4)

select r.*
,st_simplify(pg.geom,1) as geom

from results r
  join districts.2022_precinct_geoms pg
  using(lookup_key)
