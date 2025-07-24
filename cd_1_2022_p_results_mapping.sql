create table election_results.cd_1_2022_p_results_mapping as

with results as 
(select --* --distinct race, candidate
pr.county_code
, pr.precinct_name
, pr.precinct_code
, gis.lookup_key
, sum(case when candidate = 'Suzan DelBene' then votes else 0 end) as delbene_votes
, sum(case when candidate = 'Vincent J Cavaleri' then votes else 0 end) as cavaleri_votes
, sum(case when candidate in ('Vincent J Cavaleri','Matthew Heines','Derek K Chartrand') then votes else 0 end) as gop_votes


from election_results.2022_primary_precinct_results pr
  join `demswasp.commons.county_name_to_code` ctc
  on pr.county_code = ctc.countycode
    join districts.2022_precincts_gis_lookup gis 
    on gis.county_name = ctc.countyname
    and trim(lower(pr.precinct_name)) = trim(lower(gis.precinct_name))
      join districts.2022_precinct_geoms pg
      on pg.lookup_key = gis.lookup_key

where race in ('CONGRESSIONAL DISTRICT 1 - U.S. Representative','Congressional District No. 1 United States Representative')
and pr.precinct_name not in ('Total', 'Countywide')

group by 1,2,3,4)

select r.*
,st_simplify(pg.geom,1) as geom

from results r
  join districts.2022_precinct_geoms pg
  using(lookup_key)