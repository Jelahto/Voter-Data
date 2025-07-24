with target_door_count as(
    select 
        td.van_precinct_id,
        upper(trim(td.van_precinct_name)) as van_precinct_name,
        td.county_name,
        td.LD as LD,
        turf.fo_name as turf_code,
        count(distinct td.voting_address_id) as target_doors,
        sum(td.walkable_density_group) as sum_density,
        sum(td.gotv_householded) as gotv_householded,
        -- Calculate priority based on density and GOTV scores
        (sum(td.walkable_density_group) * sum(td.gotv_householded) / NULLIF(count(distinct td.voting_address_id),0)) as pct_prio
    from target_doors td
    left join demswasp.vansync.turf turf on td.van_precinct_id = turf.van_precinct_id
        and turf.committee_id = turfing_cmt_id
    group by 1,2,3,4,5
)
  
, prioritized_doors as (
    select *,
        row_number() over(order by pct_prio desc) as priority_rank -- Rank by priority score
    from target_door_count
    where pct_prio is not null
)

, cumulative_target_doors as (
    select *,
        sum(target_doors) over (order by priority_rank) as cumulative_doors -- Running total
    from prioritized_doors
)

, final_target_doors as (
    -- Select up to 55,000 doors by cumulative target
    select *
    from cumulative_target_doors
    where cumulative_doors <= 55000
)

select 
    td.van_precinct_id,
    td.van_precinct_name,
    td.county_name,
    td.LD,
    td.turf_code,
    coalesce(td.target_doors,0) as target_doors,
    coalesce(td.sum_density,0) as sum_density,
    coalesce(td.gotv_householded, 0) as sum_gotv,
    coalesce(td.sum_density,0) / NULLIF(td.target_doors,0) as pct_density,
    coalesce(td.gotv_householded,0) / NULLIF(td.target_doors,0) as pct_gotv,
    coalesce(td.sum_density,0) * coalesce(td.gotv_householded,0) / NULLIF(td.target_doors * td.target_doors,0) as pct_prio,
    coalesce(dk.doors_knocked,0) as doors_knocked,
    coalesce(dk.doors_contacts,0) as doors_contacts,
    coalesce(pc.doors_cut,0) as doors_cut
from final_target_doors td
left join precinct_door_attempts dk on td.van_precinct_id = dk.van_precinct_id
left join pcts_cut pc on td.van_precinct_id = pc.van_precinct_id
where td.van_precinct_id is not null
order by priority_rank;
