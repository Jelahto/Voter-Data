-- insert data into traffic_control.checkout
with base as (
  select 
    b.myv_van_id,
    initcap(pp.first_name) as first_name,
    initcap(pp.last_name) as last_name,
    pp.primary_cell_number as phone,
    row_number() over (
      partition by pp.primary_cell_number 
      order by pp.primary_cell_quality_score desc, 
      farm_fingerprint(b.myv_van_id) desc
    ) as phone_rank,
    pp.us_cong_district_latest as cd,
    pp.state_house_district_latest as ld,
    pp.county_name,
    target_subgroup_name,
    'Bob Ferguson for Governor' as governor_candidate,
    'Mria Cantwell for US Senator' as senator_candidate,
      case 
      when ld = '048' then 'Patty Kuderer for Insurance Commissioner'
      else null 
    end as ic_candidate,
    case 
      when (ld, cd) in (('002', '003'), ('002', '008'), ('002', '010'), 
                        ('003', '005'), ('004', '005'), ('005', '008'), 
                        ('005', '009'), ('006', '005'), ('007', '004'), 
                        ('007', '005'), ('008', '004'), ('009', '004'), 
                        ('009', '005'), ('012', '001'), ('012', '008'), 
                        ('013', '004'), ('013', '008'), ('015', '004'), 
                        ('015', '005'), ('016', '004'), ('016', '005'), 
                        ('020', '003'), ('020', '010'), ('022', '010'), 
                        ('031', '006'), ('031', '008'), ('031', '009'), 
                        ('031', '010'), ('035', '003'), ('035', '006'), 
                        ('035', '010'), ('036', '007'), ('037', '007'), 
                        ('037', '009'), ('038', '001'), ('038', '002'), 
                        ('039', '001'), ('039', '002'), ('039', '008'), 
                        ('042', '002'), ('043', '007'), ('043', '009'), 
                        ('045', '001'), ('045', '008'), ('046', '007'), 
                        ('047', '008'), ('047', '009')) 
      then 'Chris Reykdal for Superintendent of Public Instruction'
      else null 
    end as ospi_candidate,
    case 
      when (ld, cd) in (('002', '003'), ('002', '008'), ('002', '010'), 
                        ('003', '005'), ('004', '005'), ('006', '005'), 
                        ('007', '004'), ('007', '005'), ('007', '008'), 
                        ('008', '004'), ('009', '004'), ('009', '005'), 
                        ('013', '004'), ('013', '008'), ('015', '004'), 
                        ('015', '005'), ('016', '004'), ('016', '005'), 
                        ('017', '004'), ('020', '003'), ('020', '010'), 
                        ('023', '006'), ('024', '006'), ('025', '008'), 
                        ('025', '010'), ('026', '006'), ('027', '006'), 
                        ('029', '006'), ('031', '006'), ('031', '008'), 
                        ('031', '009'), ('031', '010'), ('035', '003'), 
                        ('035', '006'), ('035', '010'), ('039', '001'), 
                        ('039', '002'), ('039', '008'), ('044', '001'), 
                        ('044', '002'), ('045', '001'), ('045', '008')) 
      then 'Steve Hobbs for Secretary of State'
      else null 
    end as SOS_candidate,
    case 
      when cd = '001' then 'Suzan DelBene for US Representative'
      when cd = '002' then 'Rick Larsen for US Representative'
      when cd = '003' then 'Marie Gluesenkamp Perez for US Representative'
      when cd = '007' then 'Pramila Jayapal for US Representative'
      when cd = '008' then 'Dr. Kim Schrier for US Representative'
      when cd = '009' then 'Adam Smith for US Representative'
      when cd = '010' then 'Marilyn Strickland for US Representative'
      else null 
    end as us_representative_candidate,
    case 
      when ld = '001' then 'Derek Stanford for State Senator'
      when ld = '003' then 'Marcus Riccelli for State Senator'
      when ld = '005' then 'Bill Ramos for State Senator'
      when ld = '010' then 'Janet St. Clair for State Senator'
      when ld = '011' then 'Bob Hasegawa for State Senator'
      when ld = '012' then 'Jim Mayhew for State Senator'
      when ld = '014' then 'Maria Beltran for State Senator'
      when ld = '017' then 'Marla Keethler for State Senator'
      when ld = '018' then 'Adrian Cortes for State Senator'
      when ld = '019' then 'Andi Day for State Senator'
      when ld = '023' then 'Drew Hansen for State Senator'
      when ld = '024' then 'Mike Chapman for State Senator'
      when ld = '027' then 'Yasmin Trudeau for State Senator'
      when ld = '028' then 'T''Wina Nobles for State Senator'
      when ld = '040' then 'Liz Lovelett for State Senator'
      when ld = '041' then 'Lisa Wellman for State Senator'
      when ld = '049' then 'Annette Cleveland for State Senator'
      else null 
    end as state_senator_candidate,
    case 
      when ld = '001' then 'Davina Duerr for State Represenatitve'
      when ld = '010' then 'Clyde Shavers for State Representative'
      when ld = '011' then 'David Hackney for State Represenatitve'
      when ld = '012' then 'Heather Koellen for State Represenatitve'
      when ld = '014' then 'Chelsea Dimas for State Represenatitve'
      when ld = '018' then 'Deken Letinich for State Represenatitve'
      when ld = '019' then 'Mike Coverdale for State Represenatitve'
      when ld = '021' then 'Strom Peterson for State Represenatitve'
      when ld = '022' then 'Beth Doglio for State Represenatitve'
      when ld = '023' then 'Tarra Simmons for State Represenatitve'
      when ld = '025' then 'Cameron Severns for State Represenatitve'
      when ld = '026' then 'Adison Richards for State Represenatitve'
      when ld = '027' then 'Laurie Jinkins for State Represenatitve'
      when ld = '028' then 'Mari Leavitt for State Represenatitve'
      when ld = '029' then 'Melanie Morgan for State Represenatitve'
      when ld = '030' then 'Jamila Taylor for State Represenatitve'
      when ld = '032' then 'Cindy Ryu for State Represenatitve'
      when ld = '033' then 'Tina Orwall for State Represenatitve'
      when ld = '034' then 'Emily Alvarado for State Represenatitve'
      when ld = '036' then 'Julia Reed for State Represenatitve'
      when ld = '037' then 'Sharon Tomiko Santos for State Represenatitve'
      when ld = '038' then 'Julio Cortes for State Represenatitve'
      when ld = '040' then 'Debra Lekanoff for State Represenatitve'
      when ld = '041' then 'Tana Senn for State Represenatitve'
      when ld = '042' then 'Alicia Rule for State Represenatitve'
      when ld = '043' then 'Nicole Marci for State Represenatitve'
      when ld = '044' then 'Brandy Donaghy for State Represenatitve'
      when ld = '046' then 'Gerry Pollet for State Represenatitve'
      when ld = '047' then 'Debra Enttenman for State Represenatitve'
      when ld = '048' then 'Vandana Slatter for State Represenatitve'
      when ld = '049' then 'Sharon Wylie for State Represenatitve'
      else null 
    end as state_rep_1_candidate,
    case 
      when ld = '001' then 'Shelley Kloba for State Represenatitve'
      when ld = '003' then 'Timm Ormsby State Represenatitve'
      when ld = '005' then 'Lisa Callan for State Represenatitve'
      when ld = '010' then 'Dave Paul for State Represenatitve'
      when ld = '011' then 'Steve Bergquist for State Represenatitve'
      when ld = '014' then 'Ana Ruiz Kennedy for State Represenatitve'
      when ld = '017' then 'Terry Niles for State Represenatitve'
      when ld = '018' then 'Deken Letinich for State Represenatitve'
      when ld = '018' then 'John Zingale for State Represenatitve'
      when ld = '019' then 'Terry Carlson for State Represenatitve'
      when ld = '021' then 'Lillian Ortiz-Self for State Represenatitve'
      when ld = '023' then 'Greg Nance for State Represenatitve'
      when ld = '024' then 'Steve Tharinger for State Represenatitve'
      when ld = '025' then 'Shellie Willis for State Represenatitve'
      when ld = '027' then 'Jake Fey for State Represenatitve'
      when ld = '028' then 'Dan Bronoske for State Represenatitve'
      when ld = '029' then 'Sharlett Mena for State Represenatitve'
      when ld = '030' then 'Kristine Reeves for State Represenatitve'
      when ld = '031' then 'Brian Gunn for State Represenatitve'
      when ld = '032' then 'Lauren Davis for State Represenatitve'
      when ld = '033' then 'Mia Su-Ling for State Represenatitve'
      when ld = '034' then 'Joe Fitzgibbon for State Represenatitve'
      when ld = '036' then 'Liz Berry for State Represenatitve'
      when ld = '037' then 'Chipalo Street for State Represenatitve'
      when ld = '038' then 'Mary Fosse for State Represenatitve'
      when ld = '040' then 'Alex Ramel for State Represenatitve'
      when ld = '041' then 'My-Linh Thai for State Represenatitve'
      when ld = '042' then 'Joe Timmons for State Represenatitve'
      when ld = '043' then 'Shaun Scott for State Represenatitve'
      when ld = '044' then 'April Berg for State Represenatitve'
      when ld = '046' then 'Darya Farivar for State Represenatitve'
      when ld = '047' then 'Chris Steams for State Represenatitve'
      when ld = '048' then 'Amy Walen for State Represenatitve'
      when ld = '049' then 'Monica Jurardo or State Represenatitve'
      else null 
    end as state_rep_2_candidate
  ,case when sl.senate_candidate is not null and sl.house_1_candidate is not null and sl.house_2_candidate is not null
        then concat('Your State Senate candidate is ',sl.senate_candidate,' and your State House candidates are ',sl.house_1_candidate,' and ',sl.house_2_candidate,'.')
      when sl.senate_candidate is not null and (sl.house_1_candidate is not null or sl.house_2_candidate is not null)
        then concat('Your State Senate candidate is ',sl.senate_candidate,' and your State House candidate is ',coalesce(sl.house_1_candidate,sl.house_2_candidate),'.')
      when sl.senate_candidate is not null and sl.house_1_candidate is null and sl.house_2_candidate is null
        then concat('Your State Senate candidate is ',sl.senate_candidate,'.')
      when sl.senate_candidate is null and sl.house_1_candidate is not null and sl.house_2_candidate is not null
        then concat('Your State House candidates are ',sl.house_1_candidate,' and ',sl.house_2_candidate,'.')
      when sl.senate_candidate is null and (sl.house_1_candidate is not null or sl.house_2_candidate is not null)
        then concat('Your State House candidate is ',coalesce(sl.house_1_candidate,sl.house_2_candidate),'.')
      else null end as leg_string
  from 
    commons.dvc_targets_today b
    join democrats.analytics_wa.person pp on b.myv_van_id = pp.myv_van_id 
    left join traffic_control.checkout tc on tc.phone = pp.primary_cell_number 
      and cast(tc.checkout_expiration as date) >= current_date("america/los_angeles")
    left join traffic_control.checkout tc2 on tc2.voter_id = pp.myv_van_id
      and cast(tc2.checkout_expiration as date) >= current_date("america/los_angeles")
    join democrats.scores_wa.current_scores scr on pp.person_id = scr.person_id 
  where 
    tc.phone is null 
    and tc2.voter_id is null
    and pp.reg_voterfile_status = 'active'
    and pp.primary_cell_number is not null 
    and b.target_subgroup_name in ('mobilization dems')
    and pp.us_cong_district_latest in ('001','002','007','003','008','009','010')
),
attempted as (
  select 
    myv_van_id,
    sum(case when contact_type_name != 'no actual contact' then 1 else 0 end) as attempts
  from 
    vansync.contacts_contacts_myv ccmv
    join democrats.vansync_ref.contact_types ct using (contact_type_id)
  where 
    date(datetime_canvassed,"america/los_angeles") >= '2024-06-01'
    and committee_id in ('112164')
    and contact_type_name != 'no actual contact'
  group by 
    1
),
check as (
  select 
    b.myv_van_id,
    first_name,
    last_name,
    phone,
    cd,
    b.ld,
    county_name,
    governor_candidate,
    senator_candidate,
    us_representative_candidate,
    state_senator_candidate,
    state_rep_1_candidate,
    state_rep_2_candidate,
    row_number() over (
      partition by cd 
      order by 
      attempts asc nulls first, 
      farm_fingerprint(myv_van_id)
    ) as sbt,
    attempts,
    target_subgroup_name
  from 
    base b 
    join commons.22_general_slate sl on b.ld = sl.state_house_district_latest
    left join attempted a using(myv_van_id)
  where 
    phone_rank = 1
    and (a.attempts is null or a.attempts < 2)
)
select 
  myv_van_id,
  first_name,
  last_name,
  phone,
  senator_candidate,
  governor_candidate,
  us_representative_candidate,
  state_senator_candidate,
  state_rep_1_candidate,
  state_rep_2_candidate,
  concat(
    'can we count on you to vote ', senator_candidate, '? ',
    'can we count on you to vote ', governor_candidate, '? ',
    'finally, your democratic state senate candidate is ', state_senator_candidate, 
    ' and your democratic state house candidates are ', state_rep_1_candidate, 
    ' and ', state_rep_2_candidate, '.'
  ) as message
from 
  check
where 
  attempts is null
  and sbt <= 2000;
