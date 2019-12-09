CREATE OR REPLACE FUNCTION fn_load_tmp_policy_period_coverage(OUT out_function_status text, OUT out_function_error_message text)


RETURNS RECORD AS 
$BODY$

BEGIN

-- How to execute: 
-- select * from fn_load_tmp_policy_period_coverage()

truncate table tmp_policy_period_coverage;


insert into tmp_policy_period_coverage 
(sar_policy,
policy,
risk,
sar_major_peril,
trans_eff_date,
policy_period_start_date,
policy_period_end_date)
select 
sar_policy,
substr(sar_policy,1,10) policy,
sar_unit Risk,
sar_major_peril,
--sar_cov_eff_date,
Cast(sar_trans_eff_date as date) Trans_Eff_Date,
Case when sar_transaction in ('10','11','12') then cast (sar_trans_eff_date as date) 
else  
cast (sar_cov_eff_date as date)
end Policy_Period_Start_Date,
Case when sar_transaction in ('25') then cast(sar_trans_eff_date as date)
--substr(sar_trans_eff_date,5,2)||'-'||substr(sar_trans_eff_date,3,2)||'-'||substr(sar_trans_eff_date,1,2) -1
else  cast( sar_expiry_date as date)
--substr(sar_expiry_date,5,2)||'-'||substr(sar_expiry_date,3,2)||'-'||substr(sar_expiry_date,1,2)  
end Policy_Period_end_Date
--,
--sar_transaction,
--sar_premium
from sor_pms_vsam.sor_rec4514 
where sar_major_peril <'690'
and substr(sar_policy,1,2) = ('23')
and sar_type_bureau in ('MV','MO','MD')
--and sar_transaction  not in ('22','29')
order by sar_policy,Trans_Eff_Date;



out_function_status := 'SUCCESS';

RETURN;

EXCEPTION
  WHEN OTHERS THEN
    out_function_status := 'FAIL';
      out_function_error_message := SQLSTATE||': '||SQLERRM;
      RETURN;

END $BODY$

LANGUAGE plpgsql VOLATILE SECURITY INVOKER;
