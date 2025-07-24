--insert into traffic_control.checkout

with base as 
(
    select 
        b.myv_van_id,
        initcap(pp.first_name) as first_name,
        initcap(pp.last_name) as last_name,
        pp.primary_phone_number as phone,
        row_number() over (
            partition by pp.primary_phone_number
            order by 
                case 
                    when pp.primary_phone_connection = 'L' then pp.primary_landline_quality_score
                    when pp.primary_phone_connection = 'C' then pp.primary_cell_quality_score 
                end desc,
                farm_fingerprint(b.myv_van_id)
        ) as phone_rank,
        pp.us_cong_district_latest as cd,
        pp.state_house_district_latest as ld,
        pp.county_name,
        b.target_subgroup_name,
        tp.prio,
        dp.polling_location, 
        dp.address as db_address,
        dp.city as db_city,
        dp.location_description, 
        dp.schedule, 
        dp.date_authenticated


    from commons.dvc_targets_today b
    join democrats.analytics_wa.person pp
        on b.myv_van_id = pp.myv_van_id 
        and pp.reg_voter_flag is true 
    left join traffic_control.checkout tc
        on tc.phone = pp.primary_phone_number
        and cast(tc.checkout_expiration as date) >= current_date("America/Los_Angeles")
        and tc.contact_type not in ('text','texts')
    left join traffic_control.checkout tc2
        on tc2.voter_id = pp.myv_van_id
        and cast(tc2.checkout_expiration as date) >= current_date("America/Los_Angeles")
        and tc2.contact_type not in ('text','texts')
    join democrats.scores_wa.current_scores scr 
        on pp.person_id = scr.person_id 
    join commons.dvc_target_prio tp 
        on tp.target_subgroup_name = b.target_subgroup_name
    left join demswasp.sync_to_van.2024_dropbox_location_by_precinct dp
       on pp.van_precinct_id = dp.van_precinct_id  -- Join on van_precinct_id to get polling location info
    left join demswasp.commons.24_suppressions sup -- Suppressions table additions
        on b.myv_van_id = sup.myv_van_id



    where tc.phone is null 
        and tc2.voter_id is null
        and pp.reg_voter_flag is true
        and pp.reg_voterfile_status = 'Active'
        and pp.primary_phone_number is not null 
        and sup.myv_van_id is null  -- Exclude records present in suppressions
        and (b.target_subgroup_name in ('Mobilization Dems','Soft Dems') 
             or (us_cong_district_latest in ('003','008') and b.target_subgroup_name in ('Soft Dems','Base Dems')) 
             or (state_house_district_latest in ('014','010','042','026') and b.target_subgroup_name in ('Base Dems','Soft Dems')))
--and pp.us_cong_district_latest in ('002') and b.target_subgroup_name = 'Base Dems' -- comment out line above and use this line for standing click dialer
--and pp.us_cong_district_latest in ('005')

--and (b.target_subgroup_name in ('Mobilization Dems','Base Dems') or ((state_house_district_latest in ('005','010','024','026','029','030','038','042','044','047') or us_cong_district_latest in ('003','008')) and b.target_subgroup_name in ('Core Dems')))
)


,attempted as (
    select 
        myv_van_id,
        sum(case when contact_type_name != 'No Actual Contact' then 1 else 0 end) as attempts
    from vansync.contacts_contacts_myv ccmv
    join democrats.vansync_ref.contact_types ct
        using (contact_type_id)
    where date(datetime_canvassed, "America/Los_Angeles") >= '2024-06-01'
        and committee_id in ('112164')
        and contact_type_name != 'No Actual Contact'
    group by 1
)


, check as (
    select 
        b.myv_van_id,
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
            order by 
                case 
                    when target_subgroup_name in ('Mobilization Dems','Soft Dems') 
                    then attempts 
                    else coalesce(attempts,1) 
                end * prio asc nulls first,
                farm_fingerprint(myv_van_id)
        ) as sbt,
        attempts, 
        target_subgroup_name,
        polling_location, 
        db_address,
        db_city,
        location_description, 
        schedule


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
,polling_location
,db_address
,db_city
,location_description
,schedule

--,target_subgroup_name
--,county_name

from check


where --attempts is null
--sbt <= 20000 -- for single district 
--/* -- more dynamic targeting ratios for multi district
 ((cd in ('001') and sbt <= 4180/1)
or (cd = '002' and sbt <= 2020/1)
or (cd = '003' and sbt <= 0000/1)
or (cd = '004' and sbt <= 1530/1)
or (cd = '005' and sbt <= 1830/1)
or (cd = '006' and sbt <= 2193/1)
or (cd = '007' and sbt <= 4790/1)
or (cd = '008' and sbt <= 6353/1)
or (cd = '009' and sbt <= 4500/1)
or (cd = '010' and sbt <= 4000/1) )--*/--)

--*/
/*
select myv_van_id
,'myv_van_id' as id_type
,phone
,'Statewide GOTV Nov 1' as list_name
,'2024-11-01' as checkout_date
,'2024-11-03' as checkout_expiration
,'calls' as contact_type

from check

where --attempts is null
--sbt <= 20000 -- for single district 
--/* -- more dynamic targeting ratios for multi district
 ((cd in ('001') and sbt <= 4180/1)
or (cd = '002' and sbt <= 2020/1)
or (cd = '003' and sbt <= 0000/1)
or (cd = '004' and sbt <= 1530/1)
or (cd = '005' and sbt <= 1830/1)
or (cd = '006' and sbt <= 2193/1)
or (cd = '007' and sbt <= 4790/1)
or (cd = '008' and sbt <= 6353/1)
or (cd = '009' and sbt <= 4500/1)
or (cd = '010' and sbt <= 4000/1) )--*/--)

--*/
/*
select cd,target_subgroup_name, count(*)

from maybe

group by 1,2
order by 1,2
--*/
--*/

