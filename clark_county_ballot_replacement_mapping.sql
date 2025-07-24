create or replace table `demswasp.reporting.replacement_ballot_map_clark_county` as
with replacement_ballots as (
    select 
        safe_cast(r.voter_id as int) as voter_id,
        r.county,
        r.address,
        r.city,
        r.zip,
        r.precinct,
        r.return_location,
        r.sent_date,
        r.ballot_status,
        p.voting_address_latitude as latitude,
        p.voting_address_longitude as longitude,
        p.van_precinct_name,
        p.county_name,
        p.state_house_district_latest,
        p.us_cong_district_latest
    from `demswasp.commons.replacement_ballots_1028` r
    left join democrats.analytics_wa.person p 
        on r.voter_id = p.myv_van_id
    where r.county = 'Clark' 
      and r.sent_date >= '2024-10-28'
      and (r.ballot_status is null or r.ballot_status != 'returned')
      and safe_cast(r.voter_id as int) is not null  -- filter out non-integer voter_id values
)

-- aggregating data around precincts or locations for mapping
select
    county_name,
    van_precinct_name,
    state_house_district_latest,
    us_cong_district_latest,
    count(distinct voter_id) as voters_with_replacement_ballots,
    avg(latitude) as avg_latitude,
    avg(longitude) as avg_longitude,
    return_location
from replacement_ballots
group by 
    county_name, 
    van_precinct_name, 
    state_house_district_latest, 
    us_cong_district_latest, 
    return_location
order by voters_with_replacement_ballots desc;
