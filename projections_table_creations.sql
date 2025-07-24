
####This is the code I used to create the Projections Aggregation Table

--create or replace table `demswasp.sbx_williamsj1.projectionsaggregation2024` as
with counts as(
select 
    p.county_name,
    p.van_precinct_name,
    k.Ballot_Status,
    count(*) as reg_voters,
    sum(v.support) as dem_proj_24, --dem_share_24,
    sum(wad.walkable_density_group) as urbanicity,
    sum(case when p.ethnicity_combined = 'W' then 1 else 0 end) as white_voters,
    sum(case when p.ethnicity_combined = 'N' then 1 else 0 end) as native_voters,
    sum(case when p.ethnicity_combined = 'B' then 1 else 0 end) as black_voters,
    sum(case when p.ethnicity_combined = 'A' then 1 else 0 end) as asian_voters, 
    sum(case when p.ethnicity_combined = 'H' then 1 else 0 end) as hispanic_voters,
    sum(case when p.ethnicity_combined = 'Null' then 1 else 0 end) as unknown_race,
    sum(v.education_polling_less_than_bachelors) as non_college,
    sum(case when p.gender_combined = 'F' then 1 else 0 end) as women_voters,
    sum(case when p.gender_combined = 'M' then 1 else 0 end) as men_voters,
    avg(p.age_combined) as avg_age,
    sum(scr22.dnc_2022_dem_party_support) as dem_proj_22, --as dem_share_22,
    sum(d.dccc_support_score/100) as dem_proj_dccc --as dem_share_dccc, also divided by 100
from
    `democrats.scores_wa.current_scores` v
join
    `democrats.analytics_wa.person` p
on
    v.person_id = p.person_id
left join 
    `demswasp.commons.dccc_scores_24` d
on 
    p.myv_van_id = d.myv_van_id
left join 
    `commons.walkable_address_density` wad
on
    p.voting_address_id = wad.voting_address_id
join 
    `democrats.scores_wa.all_scores_2022` scr22
on 
    p.person_id = scr22.person_id
left join 
    `democrats.av_ev_wa_20240806_primary.WA_statewide_70_source` k
on
    p.sos_id = format('WA%09d',safe_cast(k.voter_id as int))
where 
    p.reg_voter_flag IS TRUE
group by 
    p.county_name,
    p.van_precinct_name,
    k.Ballot_Status
)

select county_name
,van_precinct_name 
,Ballot_Status
,dem_proj_24/reg_voters as dem_share_24
,dem_proj_22/reg_voters as dem_share_22
,dem_proj_dccc/reg_voters as dem_share_dccc
,men_voters/reg_voters as men_share
,white_voters/reg_voters as white_share
,native_voters/reg_voters as native_share
,black_voters/reg_voters as black_share
,asian_voters/reg_voters as asian_share
,hispanic_voters/reg_voters as hispanic_share
,unknown_race/reg_voters as unknown_share
,women_voters/reg_voters as women_share
,urbanicity/reg_voters as avg_dense_share
,non_college/reg_voters as non_college_share
,avg_age



from counts

--where Ballot_Status = 'Accepted'

############### This is the code I used to create the amended precicnt results
with vote_totals as(
select 

    county_code,
    precinct_name,
    sum(case when candidate in ('Maria Cantwell','Paul Lawrence Giesick') then votes else null end) as senator_dem_votes,
    sum(case when candidate in ('Mel Ram','Dr Raul Garcia','Isaac A Holyk','Scott Nazarino','Goodspaceguy') then votes else null end) as senator_gop_votes,
    sum(case when candidate in ('Cassondra Magdalene Hanson', 'Mark Mullet', 'EL\'ona Kearney', 'Ricky Anthony', 'Bob Ferguson', 'Chaytan Inman', 'Don L Rivers', 'Fred Grant', 'Edward Cale IV') then votes else null end) as gov_dem_votes,
    sum(case when candidate in ('Jim Daniel', 'Martin Lee Wheeler', 'Jennifer Hoover', 'Dave Reichert', 'Leon A Lawson', 'A.L. Brown', 'Bill Hirt', 'Semi Bird') then votes else null end) as gov_gop_votes,
    sum(case when candidate in ('Maria Cantwell', 'Paul Lawrence Giesick', 'Suzan DelBene', 'Edwin Stickle', 'Rick Larsen', 'Devin Hermanson', 'Josh Binda', 'Marie Gluesenkamp Perez', '   Mary Baechler', '"Birdie" Jane Muchlinski', 'Carmela Conroy', 'Ann Marie Danimus', 'Matthew Welde', 'Bernadine Bank', 'Bobbi Bennett-Wolcott', 'Emily Randall', 'Hilary Franz', '     Pramila Jayapal', 'Liz Hallock', 'Kim Schrier', 'Keith Arnold', 'Imraan Siddiqi', 'Melissa Chaudhry', 'Adam Smith', 'Marilyn Strickland', 'Eric Mahaffy', 'Desiree C. Toliver','Cassondra Magdalene Hanson', 'Mark Mullet', 'EL\'ona Kearney', 'Ricky Anthony', 'Bob Ferguson', 'Chaytan Inman', 'Don L Rivers', 'Fred Grant', 'Edward Cale IV', 'Steve Hobbs', 'Mike Pellicciotti', 'Pat (Patrice) McCarthy', 'Nick Brown', 'Allen Lebovitz', 'Dave Upthegrove', 'Patrick DePoe', 'Jeralee Anderson', 'Kevin Van De Wege', 'Patty Kuderer', 'Chris D. Chung', 'Bill Boyd', 'John Pestinger') then votes else null end) as all_slated_dem,
    sum(case when candidate in ('Mel Ram', 'Dr Raul Garcia', 'Isaac A Holyk', 'Scott Nazarino', 'Goodspaceguy', 'Orion Webster', 'Mary Silva', 'Derek Chartrand', 'Matt Heines', 'Jeb Brewer', 'Daniel Miller', 'Cody Hart', 'Leif Johnson', 'Leslie Lewallen', 'Joe Kent', 'Jerrod Sessler', 'Dan Newhouse', 'Tiffany Smiley', 'Rick Valentine Flynn', 'Rene\' Holaday', 'Jacquelin Maycumber', 'Jonathan D Bingle', 'Michael Baumgartner', 'Brian Dansel', 'Janis Clark', 'Drew C MacEwen', 'Cliff Moon', 'Carmen Goers', 'Paul Martin', 'Mark Greene', 'Nirav Sheth', 'Don Hewett', 'Jim Daniel', 'Martin Lee Wheeler', 'Jennifer Hoover', 'Dave Reichert', 'Leon A Lawson', 'A.L. Brown', 'Bill Hirt', 'Semi Bird', 'Patrick "Pat" Harman', 'Bob Hagglund', 'Dan Matthews', 'Dale Whitaker', 'Sharon Hanek', 'Matt Hawkins', 'Pete Serrano', 'Jaime Herrera Beutler', 'Sue Kuehl Pederson', 'Phil Fortunato', 'Justin Murta') then votes else null end) as all_slated_gop,
    sum(case when candidate in ('Suzan DelBene', 'Edwin Stickle', 'Rick Larsen', 'Devin Hermanson', 'Josh Binda', 
                            'Marie Gluesenkamp Perez', 'Mary Baechler', '"Birdie" Jane Muchlinski', 
                            'Carmela Conroy', 'Ann Marie Danimus', 'Matthew Welde', 'Bernadine Bank', 
                            'Bobbi Bennett-Wolcott', 'Emily Randall', 'Hilary Franz', 
                            'Pramila Jayapal', 'Liz Hallock', 'Kim Schrier', 'Keith Arnold', 
                            'Imraan Siddiqi', 'Melissa Chaudhry', 'Adam Smith', 
                            'Marilyn Strickland', 'Eric Mahaffy', 'Desiree C. Toliver') 
            then votes else null end) as all_cd_dem_votes,
    sum(case when candidate in ('Orion Webster', 'Mary Silva', 'Derek Chartrand', 'Matt Heines', 'Jeb Brewer', 
                            'Daniel Miller', 'Cody Hart', 'Leif Johnson', 'Leslie Lewallen', 'Joe Kent', 
                            'Jerrod Sessler', 'Dan Newhouse', 'Tiffany Smiley', 'Rick Valentine Flynn', 
                            'Rene\' Holaday', 'Jacquelin Maycumber', 'Jonathan D Bingle', 
                            'Michael Baumgartner', 'Brian Dansel', 'Janis Clark', 'Drew C MacEwen', 
                            'Cliff Moon', 'Carmen Goers', 'Paul Martin', 'Mark Greene', 
                            'Nirav Sheth', 'Don Hewett') 
            then votes else null end) as all_cd_gop_votes,
    sum(case when candidate in ('Orion Webster','Mary Silva','Derek Chartrand','Matt Heines','Jeb Brewer') then votes else null end) as cd1_gop_votes,
    sum(case when candidate in ('Daniel Miller', 'Cody Hart', 'Leif Johnson') then votes else null end) as cd2_gop_votes,
    sum(case when candidate in ('Leslie Lewallen', 'Joe Kent') then votes else null end) as cd3_gop_votes,
    sum(case when candidate in ('Jerrod Sessler', 'Dan Newhouse', 'Tiffany Smiley') then votes else null end) as cd4_gop_votes,
    sum(case when candidate in ('Rick Valentine Flynn', 'Rene\' Holaday', 'Jacquelin Maycumber', 'Jonathan D Bingle', 'Michael Baumgartner', 'Brian Dansel') then votes else null end) as cd5_gop_votes,
    sum(case when candidate in ('Janis Clark', 'Drew C MacEwen') then votes else null end) as cd6_gop_votes,
    sum(case when candidate in ('Cliff Moon') then votes else null end) as cd7_gop_votes,
    sum(case when candidate in ('Carmen Goers') then votes else null end) as cd8_gop_votes,
    sum(case when candidate in ('Paul Martin', 'Mark Greene') then votes else null end) as cd9_gop_votes,
    sum(case when candidate in ('Nirav Sheth', 'Don Hewett') then votes else null end) as cd10_gop_votes,
    sum(case when candidate in ('Suzan DelBene') then votes else null end) as cd1_dem_votes,
    sum(case when candidate in ('Edwin Stickle', 'Rick Larsen', 'Devin Hermanson', 'Josh Binda') then votes else null end) as cd2_dem_votes,
    sum(case when candidate in ('Marie Gluesenkamp Perez') then votes else null end) as cd3_dem_votes,
    sum(case when candidate in ('Mary Baechler', '"Birdie" Jane Muchlinski') then votes else null end) as cd4_dem_votes,
    sum(case when candidate in ('Carmela Conroy', 'Ann Marie Danimus', 'Matthew Welde', 'Bernadine Bank', 'Bobbi Bennett-Wolcott') then votes else null end) as cd5_dem_votes,
    sum(case when candidate in ('Emily Randall', 'Hilary Franz') then votes else null end) as cd6_dem_votes,
    sum(case when candidate in ('Pramila Jayapal', 'Liz Hallock') then votes else null end) as cd7_dem_votes,
    sum(case when candidate in ('Kim Schrier', 'Keith Arnold', 'Imraan Siddiqi') then votes else null end) as cd8_dem_votes,
    sum(case when candidate in ('Melissa Chaudhry', 'Adam Smith') then votes else null end) as cd9_dem_votes,
    sum(case when candidate in ('Marilyn Strickland', 'Eric Mahaffy', 'Desiree C. Toliver') then votes else null end) as cd10_dem_votes,

    sum(case when candidate = 'Maria Cantwell' then votes else null end) as cantwell_votes,
    sum(case when candidate = 'Denny Heck' then votes else null end) as heck_votes,
    sum(case when candidate = 'Bob Ferguson' then votes else null end) as ferguson_votes,
    sum(case when candidate = 'Steve Hobbs' then votes else null end) as hobbs_votes,
    sum(case when candidate = 'Mike Pellicciotti' then votes else null end) as pellicciotti_votes,
    sum(case when candidate = 'Pat (Patrice) McCarthy' then votes else null end) as mccarthy_votes,
    sum(case when candidate = 'Chris Reykdal' then votes else null end) as reykdal_votes,
    sum(case when candidate = 'Patty Kuderer' then votes else null end) as kuderer_votes,
    sum(case when candidate = 'Kim Schrier' then votes else null end) as dr_schrier_votes,
    sum(case when candidate = 'Susan DelBene' then votes else null end) as delbene_votes,
    sum(case when candidate = 'Adam Smith' then votes else null end) as smith_votes,
    sum(case when candidate = 'Marilyn Strickland' then votes else null end) as strickland_votes,

from `demswasp.election_results.2024_primary_precinct_results`

group by 
    county_code,
    precinct_name,
1,2

)

select county_code
,precinct_name
,cantwell_votes/(senator_dem_votes+senator_gop_votes) as senate_cantwell_share
,ferguson_votes/(senator_dem_votes+senator_gop_votes) as senate_ferguson_share
,hobbs_votes/(senator_dem_votes+senator_gop_votes) as senate_hobbs_share
,pellicciotti_votes/(senator_dem_votes+senator_gop_votes) as senate_pellicciotti_share
,mccarthy_votes/(senator_dem_votes+senator_gop_votes) as senate_mccarthy_share
,reykdal_votes/(senator_dem_votes+senator_gop_votes) as senate_reykdal_share
,kuderer_votes/(senator_dem_votes+senator_gop_votes) as senate_kuderer_share
,dr_schrier_votes/(senator_dem_votes+senator_gop_votes) as senate_dr_schrier_share
,delbene_votes/(senator_dem_votes+senator_gop_votes) as senate_delbene_share
,smith_votes/(senator_dem_votes+senator_gop_votes) as senate_smith_share
,strickland_votes/(senator_dem_votes+senator_gop_votes) as senate_strickland_share

,senator_dem_votes/(senator_dem_votes+senator_gop_votes) as senate_dem_share


from vote_totals


all_slated_dem
all_slated_gop
cd3_dem_votes
cd8_dem_votes
cd1_dem_votes
all_cd_dem_votes
put the above variables in the a similar sql equation based on their identifier (cd1,all_slated,etc.)
,senator_dem_votes/(senator_dem_votes+senator_gop_votes) as senate_dem_share
###### This is the code I used to create  Precinct_Results & Projections final sheet for analysis
select 
    d.county_name,
    d.van_precinct_name,
    d.dem_share_24,
    d.dem_share_22,
    d.dem_share_dccc,
    d.urbanicity,
    d.white_voters,
    d.native_voters,
    d.black_voters,
    d.asian_voters,
    d.hispanic_voters,
    d.non_college,
    d.women_voters,
    d.men_voters,
    d.avg_age,
    d.Ballot_Status,
    p.senator_dem_votes,
    p.senator_gop_votes,
    p.all_slated_dem,
    p.all_slated_gop,
    p.all_cd_dem_votes,
    p.all_cd_gop_votes,
    p.cantwell_votes
    p.ferguson_votes,
    p.hobbs_votes,
    p.pellicciotti_votes,
    p.mccarthy_votes,
    p.reykdal_votes,
    p.kuderer_votes,
    p.dr_schrier_votes,
    p.delbene_votes,
    p.smith_votes,
    p.strickland_votes
from 
    `demswasp.sbx_williamsj1.projectionsaggregation2024` as d
join 
    commons.county_name_to_code ctc
    on d.county_name = ctc.countyname
left join 
    `demswasp.sbx_williamsj1.senate_cd_precinct_level_results` as p
on 
    ctc.countycode = p.county_code
    and (lower(trim(d.van_precinct_name)) = lower(trim(p.precinct_name)) or lower(trim(d.van_precinct_name)) = concat("0",lower(trim(p.precinct_name))))

where d.Ballot_status = 'Accepted'


