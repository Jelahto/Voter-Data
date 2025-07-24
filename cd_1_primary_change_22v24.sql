create table election_results.cd_1_primary_change_22v24 as

select p24.county_name 
,p24.precinct_name 
,p24.delbene_votes as delbene_24
,p24.gop_votes as gop_24
,safe_divide(p24.delbene_votes,(p24.delbene_votes+p24.gop_votes)) as delbene_share_24
,p22.delbene_votes as delbene_22
,p22.gop_votes as gop_22
,safe_divide(p22.delbene_votes,(p22.delbene_votes+p22.gop_votes)) as delbene_share_22
,p24.geom

from `demswasp.election_results.cd_1_2024_p_results_mapping` p24
  join `demswasp.commons.county_name_to_code` ctc
  on p24.county_name = ctc.countyname
    join `demswasp.election_results.cd_1_2022_p_results_mapping` p22
    on ctc.countycode = p22.county_code
    and p24.precinct_name = p22.precinct_name