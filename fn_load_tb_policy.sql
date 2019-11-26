CREATE OR REPLACE FUNCTION fn_load_tb_policy(IN batch_name text, IN batch_number integer, IN batch_from_timestamp timestamp without time zone, IN batch_to_timestamp timestamp without time zone
                                                               ,OUT out_function_status text, OUT out_function_error_message text)


RETURNS RECORD AS 
$BODY$

BEGIN

-- Example of how to run this function: 
-- select * from public.fn_load_tb_policy(cast ('abc' as text),1,cast (now() as timestamp without time zone),cast (now() as timestamp without time zone))

--the function should get batch name as input parameter and based on that the partition should be dropped
--execute 'alter table public.tb_policy truncate partition ' || batch_name ;

truncate table tb_policy;

INSERT INTO tb_policy ( 
     source_system,
     policy_line_id,
     policy_id,
     policy_term_commencement_date,
     policy_term_period,
     policy_term_start_date,
     policy_term_expiry_date,
     policy_term_effective_date,
     cancellation_date,
     policy_line_type,
     brand,
     exposure_start_date,
     exposure_end_date
)

SELECT 
    a.md_source_system_code,
    a.policy_business_key,                 
    a.policy_number,  
    a.original_policy_inception_date, 
    a.term_number,
    a.term_start_date,                    
    a.term_end_date,                      
    a.policy_period_edit_effective_date,
    cast(a.cancellation_date as date),
    a.product_code, 
    a.brand_code,
    cast(a.cancellation_date as date),
    a.product_code, 
    a.brand_code,
    b.personal_motor_effective_date,  
    c.coverage_effective_date
FROM tb_dim_policy a 
LEFT OUTER JOIN tb_dim_coverable_personal_motor b on a.policy_business_key=b.policy_business_key
LEFT OUTER JOIN tb_dim_coverage_personal_motor c ON b.policy_business_key=c.policy_business_key
WHERE a.md_row_status='A'

--WHERE pp.updatetime >= batch_from_timestamp AND   pp.updatetime <= batch_to_timestamp;


out_function_status := 'SUCCESS';

RETURN;

EXCEPTION
  WHEN OTHERS THEN
    out_function_status := 'FAIL';
      out_function_error_message := SQLSTATE||': '||SQLERRM;
      RETURN;

END $BODY$

LANGUAGE plpgsql VOLATILE SECURITY INVOKER;

