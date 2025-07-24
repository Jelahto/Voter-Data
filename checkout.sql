create table traffic_control.checkout as

select myv_van_id as voter_id
,'myv_van_id' as id_type
,primary_phone_number as phone
,'Dialer LD 42' as list_name
,'2022-07-26' as checkout_date
,'2022-07-28' as checkout_expiration
,'calls' as contact_type

from `demswasp.traffic_control.ld42_dialer_0727`

UNION ALL 

select myv_van_id 
,'myv_van_id'
,primary_phone_number
,'Paid Calls Statewide'
,'2022-07-26'
,'2022-08-02'
,'calls'

from `demswasp.traffic_control.paid_calls_0726`

UNION ALL

select myv_van_id 
,'myv_van_id'
,primary_phone_number
,'Paid Calls CD 1'
,'2022-07-26'
,'2022-08-02'
,'calls'

from `demswasp.traffic_control.paid_calls_cd1_0726`

UNION ALL

select myv_van_id 
,'myv_van_id'
,primary_cell_number
,'Prio District Texts'
,'2022-07-26'
,'2022-08-02'
,'texts'

from `demswasp.traffic_control.texts_batch_0727`