create OR REPLACE table reporting.matchback_today_p24 as
with current_status as (
    select distinct *
    from (
        select *
        from democrats.av_ev_wa_20240806_primary.WA_statewide_2_source --redirect to most recent file after upload is processed
    )
),
counts as (
    select 
        coalesce(pp.gender_combined,'U') as gender_combined,
        CASE WHEN pp.age_combined <= 29 THEN '18-29'
            WHEN pp.age_combined BETWEEN 30 AND 44 THEN '30-44'
            WHEN pp.age_combined BETWEEN 45 AND 64 THEN '45-64'
            WHEN pp.age_combined >= 65 THEN '65+'
            ELSE 'Not Specified' END AS age,
        case when pp.ethnicity_combined = 'H' then 'Hispanic/Latino'
            when pp.ethnicity_combined = 'B' then 'African American'
            when pp.ethnicity_combined = 'W' then 'Caucasian'
            when pp.ethnicity_combined = 'A' then 'Asian American'
            when pp.ethnicity_combined = 'N' then 'Native American'
            else 'Unknown' end as ethnicity,
        concat(democrats.functions.width_bucket(least(scr.support, .99),0,1,5)*20, ' - ',democrats.functions.width_bucket (least(scr.support, .99),0,1,5)*20+20) as support_bucket,
        coalesce(concat(democrats.functions.width_bucket(scr.turnout,0,1,5)*20,' - ',democrats.functions.width_bucket(scr.turnout,0,1,5)*20+20),' Unscored') as turnout_bucket,
        cs.ballot_status,
        case when strpos(received_date,' ') = 9 then cast(concat(right(left(received_date,8),4),'-0',left(received_date,1),'-0',right(left(received_date,3),1)) as date)
            when strpos(received_date,' ') = 10 and strpos(received_date,'/') = 2 then safe_cast(concat(right(left(received_date,9),4),'-0',left(received_date,1),'-',right(left(received_date,4),2)) as date)
            when strpos(received_date,' ') = 10 and strpos(received_date,'/') = 3 then safe_cast(concat(right(left(received_date,9),4),'-',left(received_date,2),'-0',right(left(received_date,4),1)) as date)
            when strpos(received_date,' ') = 11 then cast(concat(right(left(received_date,10),4),'-',left(received_date,2),'-',right(left(received_date,5),2)) as date)
            else null end as received_date,
        initcap(cs.challenge_reason) as challenge_reason,
        pp.county_name,
        pp.us_cong_district_latest as cd,
        pp.state_house_district_latest as ld,
        concat(pp.state_fips,pp.county_fips) as county_fips_long,
        sum(case when cs.voter_id is not null then 1 else 0 end) as ballot_count,
        sum(case when cs.ballot_status in ('Accepted','Received') then 1 else 0 end) as votes_cast,
        count(*) as registered_voters,
        sum(scr.support) as dnc_24_dem_proj,
        sum(1 - scr.support) as dnc_24_gop_proj,
        sum(scr22.dnc_2022_dem_party_support) as dnc_22_dem_proj,
        sum(1 - scr22.dnc_2022_dem_party_support) as dnc_22_gop_proj,
        sum(case when pp.us_cong_district_latest in ('003', '008') then dccc_support_score else null end) as dccc_24_dem_proj,
        sum(case when pp.us_cong_district_latest in ('003', '008') then 1 - dccc_support_score else null end) as dccc_24_gop_proj,
        sum(case when pp.gender_combined = 'M' then 1 else 0 end) as men_voters,
        sum(case when pp.gender_combined = 'F' then 1 else 0 end) as women_voters,
        sum(case when pp.ethnicity_combined = 'W' then 1 else 0 end) as white_voters,
        sum(case when pp.ethnicity_combined = 'B' then 1 else 0 end) as black_voters,
        sum(case when pp.ethnicity_combined = 'H' then 1 else 0 end) as hispanic_voters,
        sum(case when pp.ethnicity_combined = 'A' then 1 else 0 end) as asian_voters,
        sum(case when pp.ethnicity_combined = 'N' then 1 else 0 end) as native_voters
    from 
        democrats.analytics_wa.person pp
        left join current_status cs
        on pp.sos_id = format('WA%09d',safe_cast(cs.voter_id as int))
        join democrats.scores_wa.current_scores scr
        on scr.person_id = pp.person_id
        join democrats.scores_wa.all_scores_2022 scr22
        on scr.person_id = scr22.person_id
        join demswasp.commons.dccc_scores_24 dc
        on pp.myv_van_id = dc.myv_van_id
    where 
        pp.reg_voter_flag = true
        and scr.support is not null
        and pp.us_cong_district_latest is not null
        and pp.state_house_district_latest is not null
        and (pp.reg_voterfile_status = 'Active' or cs.voter_id is not null)
    group by 1,2,3,4,5,6,7,8,9,10,11,12
)

select
    us_cong_district_latest,
    state_house_district_latest,
    dnc_24_dem_proj/registered_voters as dem_share_24,
    dnc_22_dem_proj/registered_voters as dem_share_22,
    dccc_24_dem_proj/registered_voters as dem_share_dccc,
    men_voters/registered_voters as men_share,
    women_voters/registered_voters as women_share,
    white_voters/registered_voters as white_share,
    native_voters/registered_voters as native_share,
    black_voters/registered_voters as black_share,
    asian_voters/registered_voters as asian_share,
    hispanic_voters/registered_voters as hispanic_share

from counts;

create OR REPLACE table reporting.matchback_today_p24 as
with current_status as (
    select distinct *
    from (
        select *
        from democrats.av_ev_wa_20240806_primary.WA_statewide_2_source --redirect to most recent file after upload is processed
    )
)
select 
    coalesce(pp.gender_combined,'U') as gender_combined,
    CASE WHEN pp.age_combined <= 29 THEN '18-29'
         WHEN pp.age_combined BETWEEN 30 AND 44 THEN '30-44'
         WHEN pp.age_combined BETWEEN 45 AND 64 THEN '45-64'
         WHEN pp.age_combined >= 65 THEN '65+'
         ELSE 'Not Specified' END AS age,
    case when pp.ethnicity_combined = 'H' then 'Hispanic/Latino'
         when pp.ethnicity_combined = 'B' then 'African American'
         when pp.ethnicity_combined = 'W' then 'Caucasian'
         when pp.ethnicity_combined = 'A' then 'Asian American'
         when pp.ethnicity_combined = 'N' then 'Native American'
         else 'Unknown' end as ethnicity,
    concat(democrats.functions.width_bucket(least(scr.support, .99),0,1,5)*20, ' - ',democrats.functions.width_bucket (least(scr.support, .99),0,1,5)*20+20) as support_bucket,
    coalesce(concat(democrats.functions.width_bucket(scr.turnout,0,1,5)*20,' - ',democrats.functions.width_bucket(scr.turnout,0,1,5)*20+20),' Unscored') as turnout_bucket,
    cs.ballot_status,
    case when strpos(received_date,' ') = 9 then cast(concat(right(left(received_date,8),4),'-0',left(received_date,1),'-0',right(left(received_date,3),1)) as date)
         when strpos(received_date,' ') = 10 and strpos(received_date,'/') = 2 then safe_cast(concat(right(left(received_date,9),4),'-0',left(received_date,1),'-',right(left(received_date,4),2)) as date)
         when strpos(received_date,' ') = 10 and strpos(received_date,'/') = 3 then safe_cast(concat(right(left(received_date,9),4),'-',left(received_date,2),'-0',right(left(received_date,4),1)) as date)
         when strpos(received_date,' ') = 11 then cast(concat(right(left(received_date,10),4),'-',left(received_date,2),'-',right(left(received_date,5),2)) as date)
         else null end as received_date,
    initcap(cs.challenge_reason) as challenge_reason,
    pp.county_name,
    pp.us_cong_district_latest as cd,
    pp.state_house_district_latest as ld,
    concat(pp.state_fips, pp.county_fips) as county_fips_long,
    sum(case when cs.voter_id is not null then 1 else 0 end) as ballot_count,
    sum(case when cs.ballot_status in ('Accepted', 'Received') then 1 else 0 end) as votes_cast,
    count(*) as registered_voters,
    sum(scr.support) as dnc_24_dem_proj,
    sum(1 - scr.support) as dnc_24_gop_proj,
    sum(scr22.dnc_2022_dem_party_support) as dnc_22_dem_proj,
    sum(1 - scr22.dnc_2022_dem_party_support) as dnc_22_gop_proj,
    sum(case when pp.us_cong_district_latest in ('003', '008') then dccc_support_score else null end) as dccc_24_dem_proj,
    sum(case when pp.us_cong_district_latest in ('003', '008') then 1 - dccc_support_score else null end) as dccc_24_gop_proj,
    sum(case when pp.gender_combined = 'M' then 1 else 0 end) as men_voters,
    sum(case when pp.gender_combined = 'F' then 1 else 0 end) as women_voters,
    sum(case when pp.ethnicity_combined = 'W' then 1 else 0 end) as white_voters,
    sum(case when pp.ethnicity_combined = 'B' then 1 else 0 end) as black_voters,
    sum(case when pp.ethnicity_combined = 'H' then 1 else 0 end) as hispanic_voters,
    sum(case when pp.ethnicity_combined = 'A' then 1 else 0 end) as asian_voters,
    sum(case when pp.ethnicity_combined = 'N' then 1 else 0 end) as native_voters
    (sum(scr.support) / count(*)) as dem_share_24,
    (sum(scr22.dnc_2022_dem_party_support) / count(*)) as dem_share_22,
    (sum(case when pp.us_cong_district_latest in ('003', '008') then dccc_support_score else null end) / count(*)) as dem_share_dccc,
    (sum(case when pp.gender_combined = 'M' then 1 else 0 end) / count(*)) as men_share,
    (sum(case when pp.gender_combined = 'F' then 1 else 0 end) / count(*)) as women_share,
    (sum(case when pp.ethnicity_combined = 'W' then 1 else 0 end) / count(*)) as white_share,
    (sum(case when pp.ethnicity_combined = 'B' then 1 else 0 end) / count(*)) as black_share,
    (sum(case when pp.ethnicity_combined = 'H' then 1 else 0 end) / count(*)) as hispanic_share,
    (sum(case when pp.ethnicity_combined = 'A' then 1 else 0 end) / count(*)) as asian_share,
    (sum(case when pp.ethnicity_combined = 'N' then 1 else 0 end) / count(*)) as native_share

from 
    democrats.analytics_wa.person pp
    left join current_status cs
        on pp.sos_id = format('WA%09d', safe_cast(cs.voter_id as int))
    join democrats.scores_wa.current_scores scr
        on scr.person_id = pp.person_id
    join democrats.scores_wa.all_scores_2022 scr22
        on scr.person_id = scr22.person_id
    join demswasp.commons.dccc_scores_24 dc
        on pp.myv_van_id = dc.myv_van_id
where 
    pp.reg_voter_flag = true
    and scr.support is not null
    and pp.us_cong_district_latest is not null
    and pp.state_house_district_latest is not null
    and (pp.reg_voterfile_status = 'Active' or cs.voter_id is not null)
group by 
    pp.gender_combined, pp.age_combined, pp.ethnicity_combined, scr.support, scr.turnout, cs.ballot_status, pp.county_name, pp.us_cong_district_latest, pp.state_house_district_latest, pp.state_fips, pp.county_fips, cs.challenge_reason;

