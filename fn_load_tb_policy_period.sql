CREATE OR REPLACE FUNCTION fn_load_tb_policy_period(OUT out_function_status text, OUT out_function_error_message text)


RETURNS RECORD AS 
$BODY$

BEGIN

truncate table tb_policy_period;

with cte1 as (
  select sar_policy as policy_term_number,
  cast (sar_trans_eff_date as date) as sar_trans_eff_date, 
  cast (sar_expiry_date as date) as sar_expiry_date,
  cast (sar_cov_eff_date as date) as sar_cov_eff_date,
  sar_transaction
  from sor_pms_vsam.sor_rec4514 
  where substr(sar_policy,1,10)='23A0231594'
  group by policy_term_number, sar_trans_eff_date, sar_expiry_date, sar_transaction, sar_cov_eff_date
  order by policy_term_number, sar_trans_eff_date, sar_expiry_date,sar_transaction, sar_cov_eff_date
)

INSERT INTO tb_policy_period ( 
policy_term_number,
coverable_id,
coverage_id,
policy_period_start,
policy_period_end,
flag_active_row 
)

SELECT policy_term_number,
case when sar_transaction in ('10','11','12') then sar_trans_eff_date 
     else sar_cov_eff_date
     end as policy_period_start,
case when sar_transaction in ('10','11','12') then sar_expiry_date
     else sar_trans_eff_date
     end as policy_period_end,
case when sar_transaction in ('10','11','12') then 'Y'
     else 'N'
     end as flag_active_row
from cte1
where sar_transaction not in ('53','63')
order by policy_term_number,policy_period_start;

out_function_status := 'SUCCESS';

RETURN;

EXCEPTION
  WHEN OTHERS THEN
    out_function_status := 'FAIL';
      out_function_error_message := SQLSTATE||': '||SQLERRM;
      RETURN;

END $BODY$

LANGUAGE plpgsql VOLATILE SECURITY INVOKER;
