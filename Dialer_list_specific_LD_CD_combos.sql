select ld, cd
from `commons.24_general_dialer_gotv`
where lower(`q0`) like 'nick brown%' 
   or lower(`q1`) like '%nick brown%'
   or lower(`q2`) like '%nick brown%'
   or lower(`q3`) like '%nick brown%'
group by ld, cd
order by ld, cd;

-----------

-- insert into traffic_control.checkout

with base as 
(
    select b.myv_van_id,
        initcap(pp.first_name) as first_name,
        initcap(pp.last_name) as last_name,
        pp.primary_phone_number as phone,
        row_number() over (
            partition by pp.primary_phone_number
            order by case 
                when pp.primary_phone_connection = 'L' then pp.primary_landline_quality_score
                when pp.primary_phone_connection = 'C' then pp.primary_cell_quality_score 
            end desc,
            farm_fingerprint(b.myv_van_id)
        ) as phone_rank,
        pp.us_cong_district_latest as cd,
        pp.state_house_district_latest as ld,
        pp.county_name,
        b.target_subgroup_name,
        tp.prio
    from commons.dvc_targets_today b
    join democrats.analytics_wa.person pp
        on b.myv_van_id = pp.myv_van_id 
        and pp.reg_voter_flag is true
    left join traffic_control.checkout tc
        on tc.phone = pp.primary_phone_number 
        and cast(tc.checkout_expiration as date) >= current_date("America/Los_Angeles")
    left join traffic_control.checkout tc2
        on tc2.voter_id = pp.myv_van_id
        and cast(tc.checkout_expiration as date) >= current_date("America/Los_Angeles")
    join democrats.scores_wa.current_scores scr 
        on pp.person_id = scr.person_id 
    join commons.dvc_target_prio tp 
        on tp.target_subgroup_name = b.target_subgroup_name
    left join demswasp.commons.24_suppressions sup 
        on b.myv_van_id = sup.myv_van_id
    where tc.phone is null 
        and tc2.voter_id is null
        and pp.reg_voter_flag is true
        and pp.reg_voterfile_status = 'Active'
        and pp.primary_phone_number is not null 
        and sup.myv_van_id is null  -- Exclude records in suppressions
        and (b.target_subgroup_name in ('Mobilization Dems') 
             or (us_cong_district_latest in ('003','008') and b.target_subgroup_name in ('Soft Dems','Base Dems')) 
             or (state_house_district_latest in ('014','010','042','026') and b.target_subgroup_name in ('Base Dems','Soft Dems')))
        and ((pp.state_house_district_latest = '013' and pp.us_cong_district_latest = '004')
             or (pp.state_house_district_latest = '013' and pp.us_cong_district_latest = '008')
             or (pp.state_house_district_latest = '014' and pp.us_cong_district_latest = '004')
             or (pp.state_house_district_latest = '015' and pp.us_cong_district_latest = '004')
             or (pp.state_house_district_latest = '016' and pp.us_cong_district_latest = '004')
             or (pp.state_house_district_latest = '017' and pp.us_cong_district_latest = '003')
             or (pp.state_house_district_latest = '017' and pp.us_cong_district_latest = '004')
             or (pp.state_house_district_latest = '018' and pp.us_cong_district_latest = '003')
             or (pp.state_house_district_latest = '019' and pp.us_cong_district_latest = '003')
             or (pp.state_house_district_latest = '002' and pp.us_cong_district_latest = '010')
             or (pp.state_house_district_latest = '002' and pp.us_cong_district_latest = '003')
             or (pp.state_house_district_latest = '002' and pp.us_cong_district_latest = '008')
             or (pp.state_house_district_latest = '020' and pp.us_cong_district_latest = '010')
             or (pp.state_house_district_latest = '020' and pp.us_cong_district_latest = '003')
             or (pp.state_house_district_latest = '021' and pp.us_cong_district_latest = '001')
             or (pp.state_house_district_latest = '021' and pp.us_cong_district_latest = '002')
             or (pp.state_house_district_latest = '026' and pp.us_cong_district_latest = '006')
             or (pp.state_house_district_latest = '029' and pp.us_cong_district_latest = '010')
             or (pp.state_house_district_latest = '029' and pp.us_cong_district_latest = '006')
             or (pp.state_house_district_latest = '032' and pp.us_cong_district_latest = '001')
             or (pp.state_house_district_latest = '032' and pp.us_cong_district_latest = '002')
             or (pp.state_house_district_latest = '032' and pp.us_cong_district_latest = '007')
             or (pp.state_house_district_latest = '034' and pp.us_cong_district_latest = '007')
             or (pp.state_house_district_latest = '034' and pp.us_cong_district_latest = '009')
             or (pp.state_house_district_latest = '035' and pp.us_cong_district_latest = '003')
             or (pp.state_house_district_latest = '045' and pp.us_cong_district_latest = '001')
             or (pp.state_house_district_latest = '045' and pp.us_cong_district_latest = '008')
             or (pp.state_house_district_latest = '049' and pp.us_cong_district_latest = '003')
             or (pp.state_house_district_latest = '007' and pp.us_cong_district_latest = '004')
             or (pp.state_house_district_latest = '007' and pp.us_cong_district_latest = '005')
             or (pp.state_house_district_latest = '007' and pp.us_cong_district_latest = '008')
             or (pp.state_house_district_latest = '008' and pp.us_cong_district_latest = '004')
             or (pp.state_house_district_latest = '009' and pp.us_cong_district_latest = '004'))
),

attempted as (
    select myv_van_id,
        sum(case when contact_type_name != 'No Actual Contact' then 1 else 0 end) as attempts
    from vansync.contacts_contacts_myv ccmv
    join democrats.vansync_ref.contact_types ct
        using (contact_type_id)
    where date(datetime_canvassed, "America/Los_Angeles") >= '2024-06-01'
        and committee_id in ('112164')
        and contact_type_name != 'No Actual Contact'
    group by 1
),

check as (
    select b.myv_van_id,
        first_name, 
        last_name, 
        phone,
        b.cd,
        b.ld,
        county_name,
        q0 as q0_string,
        q1 as q1_string,
        q2 as q2_string,
        q3 as q3_string,
        Q0_type as Pos_Q0,
        Q1_type as Pos_Q1,
        Q2_type as Pos_Q2,
        Q3_type as Pos_Q3,
        row_number() over (
            partition by b.cd
            order by case 
                when target_subgroup_name in ('Mobilization Dems', 'Soft Dems') then attempts 
                else coalesce(attempts, 1) 
            end * prio asc nulls first,
            farm_fingerprint(myv_van_id)
        ) as sbt,
        attempts, 
        target_subgroup_name
    from base b
    join commons.24_general_dialer_gotv sl
        on b.ld = sl.ld_std
        and b.cd = sl.cd_std
    left join attempted a
        using(myv_van_id)
    where phone_rank = 1
        and ((a.attempts is null) or ((attempts * prio) <= 2.2))

--and a.attempts is null

)
--/*
--,maybe as (
select myv_van_id
,first_name 
,last_name 
,phone
,q0_string
,q1_string
,q2_string
,q3_string
--,db_name
--,db_notes
--,db_address
--,db_city
,cd
,ld
,Pos_Q0
,Pos_Q1
,Pos_Q2
,Pos_Q3

--,target_subgroup_name
--,county_name

from check


where --attempts is null
--sbt <= 10000 -- for single district 
--/* -- more dynamic targeting ratios for multi district
 ((cd in ('001') and sbt <= 2180)
or (cd = '002' and sbt <= 1920)
or (cd = '003' and sbt <= 2133)
or (cd = '004' and sbt <= 1530)
or (cd = '005' and sbt <= 1830)
or (cd = '006' and sbt <= 2193)
or (cd = '007' and sbt <= 4790)
or (cd = '008' and sbt <= 6353)
or (cd = '009' and sbt <= 4490)
or (cd = '010' and sbt <= 2590) )--*/--)

--*/
/*
select myv_van_id
,'myv_van_id' as id_type
,phone
,'October 12 CD3 Dialer' as list_name
,'2024-10-12' as checkout_date
,'2024-10-14' as checkout_expiration
,'calls' as contact_type

from check

where --attempts is null
--sbt <= 10000 -- for single district 
--/* -- more dynamic targeting ratios for multi district
 ((cd in ('001') and sbt <= 2180)
or (cd = '002' and sbt <= 1920)
or (cd = '003' and sbt <= 2133)
or (cd = '004' and sbt <= 1530)
or (cd = '005' and sbt <= 1830)
or (cd = '006' and sbt <= 2193)
or (cd = '007' and sbt <= 4790)
or (cd = '008' and sbt <= 6353)
or (cd = '009' and sbt <= 4490)
or (cd = '010' and sbt <= 2590) )--*/--)

--*/
/*
select cd,target_subgroup_name, count(*)

from maybe

group by 1,2
order by 1,2
--*/
--*/
