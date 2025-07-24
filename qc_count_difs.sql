--quick matchback QC check
with name as (
select 
'yesterday' as batch
,count(*) as ballots_returned
,sum(case when ballot_status = 'Accepted'then 1 else 0 end) as ballots_accepted
,sum(case when ballot_status = 'Received'then 1 else 0 end) as ballots_on_hand
,sum(case when ballot_status = 'Rejected'then 1 else 0 end) as ballots_challenged

from democrats.av_ev_wa_20240806_primary.WA_statewide_45_source --yesterday's report or union all if multiple files
/*
UNION ALL 

select 
'yesterday' as batch
,count(*) as ballots_returned
,sum(case when ballot_status = 'Accepted'then 1 else 0 end) as ballots_accepted
,sum(case when ballot_status = 'Received'then 1 else 0 end) as ballots_on_hand
,sum(case when ballot_status = 'Rejected'then 1 else 0 end) as ballots_challenged

from democrats.av_ev_wa_20220802_primary.WA_statewide_19_source --yesterday's report or union all if multiple files

UNION ALL 

select 
'yesterday' as batch
,count(*) as ballots_returned
,sum(case when ballot_status = 'Accepted'then 1 else 0 end) as ballots_accepted
,sum(case when ballot_status = 'Received'then 1 else 0 end) as ballots_on_hand
,sum(case when ballot_status = 'Rejected'then 1 else 0 end) as ballots_challenged

from democrats.av_ev_wa_20220802_primary.WA_statewide_20_source -- today's report or union all if multiple files

UNION ALL 

select 
'today' as batch
,count(*) as ballots_returned
,sum(case when ballot_status = 'Accepted'then 1 else 0 end) as ballots_accepted
,sum(case when ballot_status = 'Received'then 1 else 0 end) as ballots_on_hand
,sum(case when ballot_status = 'Rejected'then 1 else 0 end) as ballots_challenged

from democrats.av_ev_wa_20220802_primary.WA_statewide_21_source -- today's report or union all if multiple files

UNION ALL 

select 
'today' as batch
,count(*) as ballots_returned
,sum(case when ballot_status = 'Accepted'then 1 else 0 end) as ballots_accepted
,sum(case when ballot_status = 'Received'then 1 else 0 end) as ballots_on_hand
,sum(case when ballot_status = 'Rejected'then 1 else 0 end) as ballots_challenged

from democrats.av_ev_wa_20220802_primary.WA_statewide_22_source -- today's report or union all if multiple files
*/
UNION ALL

select 
'today' as batch
,count(*) as ballots_returned
,sum(case when ballot_status = 'Accepted'then 1 else 0 end) as ballots_accepted
,sum(case when ballot_status = 'Received'then 1 else 0 end) as ballots_on_hand
,sum(case when ballot_status = 'Rejected'then 1 else 0 end) as ballots_challenged

from democrats.av_ev_wa_20240806_primary.WA_statewide_46_source -- today's report or union all if multiple files
)

select batch 
,sum(ballots_returned) as ballots_returned
,sum(ballots_accepted) as ballots_accepted
,sum(ballots_on_hand) as ballots_on_hand
,sum(ballots_challenged) as ballots_challenged

from name

group by 1
order by 1 desc