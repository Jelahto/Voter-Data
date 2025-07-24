--insert into traffic_control.checkout

with base as 
(select b.myv_van_id
,initcap(pp.first_name) as first_name
,initcap(pp.last_name) as last_name
/* --Lands and Cells
,pp.primary_phone_number as phone
,row_number()over(
  partition by pp.primary_phone_number
  order by case when pp.primary_phone_connection = 'L' then pp.primary_landline_quality_score
                when pp.primary_phone_connection = 'C' then pp.primary_cell_quality_score end DESC
                ,FARM_FINGERPRINT(b.myv_van_id)
) as phone_rank
--*/
--/* -- Cells only
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
,db_name -- add back dropboxes later
,db.db_notes
,db.db_address
,db.db_city
,concat(ifnull(concat(' ',a.street_pre_dir),''),ifnull(concat(' ',street_name),''), ifnull(concat(' ',street_type),'')) as reg_street
--,scr. --could carry scores for prio
,b.target_subgroup_name 
,tp.prio


from commons.dvc_targets_today b
  join democrats.analytics_wa.person pp
  on b.myv_van_id = pp.myv_van_id 
  and pp.reg_voter_flag is true 
    join commons.best_dropbox_24 db  --we can add drop boxes later, more important for general GOTV
    on db.person_id = pp.person_id
      left join traffic_control.checkout tc
      on tc.phone = pp.primary_cell_number -- use pp.primary_phone_number for dialer lists, pp.primary_cell_number for text lists
      and cast(tc.checkout_expiration as date) >= '2024-10-30'
      and tc.contact_type in ('text','texts')
        left join traffic_control.checkout tc2
        on tc2.voter_id = pp.myv_van_id
        and cast(tc2.checkout_expiration as date) >= '2024-10-30'
        and tc2.contact_type in ('text','texts')
          join democrats.scores_wa.current_scores scr 
          on pp.person_id = scr.person_id 
            join commons.dvc_target_prio tp 
            on tp.target_subgroup_name = b.target_subgroup_name
              join democrats.voter_file_wa.address a 
              on pp.voting_address_id = a.address_id

where tc.phone is null 
and tc2.voter_id is null
and pp.reg_voter_flag is true
and pp.reg_voterfile_status = 'Active'
and pp.primary_phone_number is not null --cell for text phone for dialer
and (b.target_subgroup_name in ('Mobilization Dems','Base Dems','Core Dems'))
and (us_cong_district_latest != '004' or state_house_district_latest = '014')


)
,attempted as(
select myv_van_id
,sum(case when contact_type_name != 'No Actual Contact' then 1 else 0 end) as attempts

from vansync.contacts_contacts_myv ccmv
  join democrats.vansync_ref.contact_types ct
  using (contact_type_id)

where date(datetime_canvassed,"America/Los_Angeles") >= '2024-06-01'
and committee_id in ('112164')
and contact_type_id = '37'
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
,db_name --drop box elements for when that's added
,db_notes
,db_address
,db_city
,reg_street
,cand_1
,cand_2
,cand_3
,additional_dems 
,row_number()over(
  partition by b.cd --comment out for non-CD targeting
  order by 
  case when target_subgroup_name in ('Mobilization Dems','Soft Dems') then attempts else coalesce(attempts,1) end * prio asc nulls first
  ,farm_fingerprint(myv_van_id)
) as sbt
,attempts 
,target_subgroup_name


from base b
  join commons.24_general_text_slate sl
  on b.ld = sl.ld
  and b.cd = sl.cd
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
,cand_1
,cand_2
,cand_3
,additional_dems
,db_name
,db_notes
,db_address
,db_city
,reg_street
,cd
,ld


--,target_subgroup_name
--,county_name

from check


where --attempts is null
--sbt <= 10000 -- for single district 
--/* -- more dynamic targeting ratios for multi district
 ((cd in ('001') and sbt <= 2180/3)
or (cd = '002' and sbt <= 1920/3)
or (cd = '003' and sbt <= 2133/3)
or (cd = '004' and sbt <= 1530/3)
or (cd = '005' and sbt <= 1830/3)
or (cd = '006' and sbt <= 2193/3)
or (cd = '007' and sbt <= 4790/3)
or (cd = '008' and sbt <= 6353/3)
or (cd = '009' and sbt <= 4490/3)
or (cd = '010' and sbt <= 2590/3) )--*/--)

--*/
/*
select myv_van_id
,'myv_van_id' as id_type
,phone
,'October 25 Statewide Drop Box Texts' as list_name
,'2024-10-25' as checkout_date
,'2024-10-29' as checkout_expiration
,'texts' as contact_type

from check

where --attempts is null
--sbt <= 10000 -- for single district 
--/* -- more dynamic targeting ratios for multi district
 ((cd in ('001') and sbt <= 2180/3)
or (cd = '002' and sbt <= 1920/3)
or (cd = '003' and sbt <= 2133/3)
or (cd = '004' and sbt <= 1530/3)
or (cd = '005' and sbt <= 1830/3)
or (cd = '006' and sbt <= 2193/3)
or (cd = '007' and sbt <= 4790/3)
or (cd = '008' and sbt <= 6353/3)
or (cd = '009' and sbt <= 4490/3)
or (cd = '010' and sbt <= 2590/3) )--*/--)

--*/
/*
select cd,target_subgroup_name, count(*)

from maybe

group by 1,2
order by 1,2
--*/
--*/