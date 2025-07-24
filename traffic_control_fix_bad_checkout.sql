create or replace table sbx_williamsj1.tc_fix1 as

select voter_id
,id_type
,phone
,list_name
,checkout_date
,case when list_name = 'CD3 GOTV Weekend b1'then '2024-11-03' else checkout_expiration end as checkout_expiration
,contact_type

from traffic_control.checkout

name fo checkout table: `demswasp.traffic_control.checkout` '
