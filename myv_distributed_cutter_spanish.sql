--insert into traffic_control.checkout

with base as 
(select b.myv_van_id
,initcap(pp.first_name) as first_name
,initcap(pp.last_name) as last_name
--/* --Lands and Cells
,pp.primary_phone_number as phone
,row_number()over(
  partition by pp.primary_phone_number
  order by case when pp.primary_phone_connection = 'L' then pp.primary_landline_quality_score
                when pp.primary_phone_connection = 'C' then pp.primary_cell_quality_score end DESC
                ,FARM_FINGERPRINT(b.myv_van_id)
) as phone_rank
--*/
/* -- Cells only
,pp.primary_cell_number as phone 
,row_number()over(
  partition by pp.primary_cell_number 
  order by pp.primary_cell_quality_score desc
  ,FARM_FINGERPRINT(b.myv_van_id) desc
) as phone_rank
--*/
,pp.us_cong_district_latest as cd
,pp.state_house_district_latest as ld
,pp.county_name
--,db.polling_location as db_name -- add back dropboxes later
--,db.location_description as db_notes
--,db.address as db_address
--,db.city as db_city
--,scr. --could carry scores for prio
,b.target_subgroup_name 
,tp.prio


from commons.dvc_targets_today b
  join democrats.analytics_wa.person pp
  on b.myv_van_id = pp.myv_van_id 
  and pp.reg_voter_flag is true 
--    join commons.2022_best_dropbox_by_precinct db  --we can add drop boxes later, more important for general GOTV
--    on db.van_precinct_id = pp.van_precinct_id
      left join traffic_control.checkout tc
      on tc.phone = pp.primary_phone_number -- use pp.primary_phone_number for dialer lists, pp.primary_cell_number for text lists
      and cast(tc.checkout_expiration as date) >= current_date("America/Los_Angeles")  
        left join traffic_control.checkout tc2
        on tc2.voter_id = pp.myv_van_id
        and cast(tc.checkout_expiration as date) >= current_date("America/Los_Angeles")
          join democrats.scores_wa.current_scores scr 
          on pp.person_id = scr.person_id 
            join commons.dvc_target_prio tp 
            on tp.target_subgroup_name = b.target_subgroup_name

where tc.phone is null 
and tc2.voter_id is null
and pp.reg_voter_flag is true
and pp.reg_voterfile_status = 'Active'
and pp.primary_phone_number is not null --cell for text phone for dialer
and (b.target_subgroup_name in ('Mobilization Dems') or (us_cong_district_latest in ('003','008') and b.target_subgroup_name in ('Soft Dems')) or (state_house_district_latest in ('014','010','042','026') and b.target_subgroup_name in ('Base Dems','Soft Dems')))
--and pp.us_cong_district_latest in ('007')

--and (b.target_subgroup_name in ('Mobilization Dems','Base Dems') or ((state_house_district_latest in ('005','010','024','026','029','030','038','042','044','047') or us_cong_district_latest in ('003','008')) and b.target_subgroup_name in ('Core Dems')))


)
,attempted as(
select myv_van_id
,sum(case when contact_type_name != 'No Actual Contact' then 1 else 0 end) as attempts

from vansync.contacts_contacts_myv ccmv
  join democrats.vansync_ref.contact_types ct
  using (contact_type_id)

where date(datetime_canvassed,"America/Los_Angeles") >= '2024-06-01'
and committee_id in ('112164')
and contact_type_name != 'No Actual Contact'
group by 1
)
, check as (
select b.myv_van_id
,first_name 
,last_name 
,phone
,b.cd
,b.ld
,county_name
--,db_name --drop box elements for when that's added
--,db_notes
--,db_address
--,db_city
,q0 as q0_string
,q1 as q1_string
,q2 as q2_string
,q3 as q3_string
,q4 as q4_string
,Q0_type as Pos_Q0
,Q1_type as Pos_Q1
,Q2_type as Pos_Q2
,Q3_type as Pos_Q3
,Q4_type as Pos_Q4
,row_number()over(
  partition by b.cd --comment out for non-CD targeting
  order by 
  case when target_subgroup_name in ('Mobilization Dems','Soft Dems') then attempts else coalesce(attempts,1) end * prio asc nulls first
  ,farm_fingerprint(myv_van_id)
) as sbt
,attempts 
,target_subgroup_name


from base b 
  join commons.24_general_dialer_gotv_spanish sl
  on b.ld = sl.ld_std
  and b.cd = sl.cd_std
    left join attempted a
    using(myv_van_id)

where phone_rank = 1
and ((a.attempts is null) or ((attempts * prio)<=2.2))
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
--and sbt<= 25000
-- /* -- more dynamic targeting ratios for later
 ((cd in ('001') and sbt <= 2188)
or (cd = '002' and sbt <= 1920)
or (cd = '003' and sbt <= 4136)
or (cd = '004' and sbt <= 1539)
or (cd = '005' and sbt <= 1830)
or (cd = '006' and sbt <= 2190)
or (cd = '007' and sbt <= 4795)
or (cd = '008' and sbt <= 4357)
or (cd = '009' and sbt <= 4497)
or (cd = '010' and sbt <= 2595) )--*/--)

--*/
/*
select myv_van_id
,'myv_van_id'
,phone
,'October 04 Statewide Mob'
,'2024-10-04'
,'2024-10-09'
,'calls'

from check

where --attempts is null
--and sbt<= 25000
-- /* -- more dynamic targeting ratios for later
 ((cd in ('001') and sbt <= 2188)
or (cd = '002' and sbt <= 1920)
or (cd = '003' and sbt <= 4136)
or (cd = '004' and sbt <= 1539)
or (cd = '005' and sbt <= 1830)
or (cd = '006' and sbt <= 2190)
or (cd = '007' and sbt <= 4795)
or (cd = '008' and sbt <= 4357)
or (cd = '009' and sbt <= 4497)
or (cd = '010' and sbt <= 2595) )--*/--)

--*/
/*
select cd,target_subgroup_name, count(*)

from maybe

group by 1,2
order by 1,2
--*/
