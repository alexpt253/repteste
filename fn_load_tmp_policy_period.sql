CREATE OR REPLACE FUNCTION fn_load_tmp_policy_period(OUT out_function_status text, OUT out_function_error_message text)


RETURNS RECORD AS 
$BODY$

BEGIN

-- How to execute: 
-- select * fromfn_load_tmp_policy_period()

truncate table tmp_policy_period;


INSERT INTO tmp_policy_period (
policy_term_number,
policy_period_start,
policy_period_end,
flag_active_row
)

with cte1 as (
  with cte2 as (

  select sar_policy as policy_term_number, 
  case when sar_transaction in ('10','11','12') then sar_trans_eff_date 
     else sar_cov_eff_date
     end as policy_period_start,
  case when sar_transaction in ('10','11','12') then sar_expiry_date
     else sar_trans_eff_date
     end as policy_period_end,
  sar_transaction,
  cast (sar_cov_eff_date as date) as sar_cov_eff_date
  from sor_pms_vsam.sor_rec4514
--  where substr(sar_policy,1,10) in ('23A0231594','23A4110689') 
 -- where substr(sar_policy,1,10) ='23A4110689' 
  where sar_major_peril <'600'
  and substr(sar_policy,1,2) = ('23')
  and sar_type_bureau in ('MV','MO','MD') 
  and sar_transaction not in ('53','63')
  and substr(sar_trans_eff_date,1,2) in ('18','19')
  group by policy_term_number, policy_period_start, policy_period_end, sar_transaction, sar_cov_eff_date
  order by policy_term_number, policy_period_start, policy_period_end, sar_transaction, sar_cov_eff_date
  )
  
  select policy_term_number,
  cast (policy_period_start as date) as policy_period_start,
  cast (policy_period_end as date) as policy_period_end,
  sar_cov_eff_date,
  cast (lag(policy_period_start,1) over(order by policy_term_number, policy_period_start, policy_period_end) as date) as previous_policy_period_start,
  cast (lag(policy_period_end,1) over(order by policy_term_number, policy_period_start, policy_period_end) as date) as previous_policy_period_end,
  sar_transaction
  from cte2
  order by policy_term_number, policy_period_start, policy_period_end)

select policy_term_number,
policy_period_start,
policy_period_end,
--previous_policy_period_start,
--previous_policy_period_end,
case when previous_policy_period_start = policy_period_start AND previous_policy_period_end < policy_period_end then 'N'
     else 'Y'
     end as flag_active_row

from cte1
order by policy_term_number,policy_period_start, policy_period_end;

out_function_status := 'SUCCESS';

RETURN;

EXCEPTION
  WHEN OTHERS THEN
    out_function_status := 'FAIL';
      out_function_error_message := SQLSTATE||': '||SQLERRM;
      RETURN;

END $BODY$

LANGUAGE plpgsql VOLATILE SECURITY INVOKER;
