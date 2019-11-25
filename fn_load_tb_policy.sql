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
    tb_dim_policy.md_source_system_code,
    tb_dim_policy.policy_business_key,                 
    tb_dim_policy.policy_number,  
    tb_dim_policy.original_policy_inception_date, 
    tb_dim_policy.term_number,
    tb_dim_policy.term_start_date,                    
    tb_dim_policy.term_end_date,                      
    tb_dim_policy.policy_period_edit_effective_date,
    cast(tb_dim_policy.cancellation_date as date),
    tb_dim_policy.product_code, 
    tb_dim_policy.brand_code,
    '2010-01-01',-- exposure start date,
    '2010-01-01' -- exposure end date
FROM tb_dim_policy
WHERE md_row_status='A';

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

