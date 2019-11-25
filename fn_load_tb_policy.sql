CREATE OR REPLACE FUNCTION summarization.fn_load_tb_policy_stg(IN batch_name text, IN batch_number integer, IN batch_from_timestamp timestamp without time zone, IN batch_to_timestamp timestamp without time zone
                                                               ,OUT out_function_status text, OUT out_function_error_message text)


RETURNS RECORD AS 
$BODY$

BEGIN


--the function should get batch name as input parameter and based on that the partition should be dropped
--execute 'alter table public.tb_policy truncate partition ' || batch_name ;

truncate table tb_policy;

INSERT INTO tb_policy ( 
     policy_line_id
   , policy_id
--    , policy_status_code
--    , policy_status_desc
--    , policy_status_created_date
--    , policy_inception_date
    , policy_commencement_date
--    , issue_date
    , policy_term_period
    , policy_term_start_date
    , policy_term_expiry_date
--    , model_number
    , policy_term_effective_date
--    , most_recent_model
--    , term_type_code
--    , term_type_desc
    , cancellation_date
--    , cancellation_reason_code
--    , cancellation_reason_desc
--    , billing_method_code
--    , billing_method_desc
--    , primary_insured_name
--    , policy_producer_code
--    , policy_producer_desc
--   , market_segment_code
--    , market_segment_desc
--    , package_risk_code
--    , package_risk_desc
    , policy_line_type
--    , jurisdiction_code
 --   , jurisdiction_desc
    , brand
 --   , brand_name
 --   , non_renewal_reason_code
  --  , non_renewal_reason_desc
  --  , cancelled_by_name
  --  , retired
 --   , md_row_effective_date
 --   , md_source_system_code
)

SELECT 
    tb_dim_policy.policy_business_key,                 
    tb_dim_policy.policy_number,  
    tb_dim_policy.original_policy_inception_date, 
    tb_dim_policy.term_number,
    tb_dim_policy.term_start_date,                    
    tb_dim_policy.term_end_date,                      
    tb_dim_policy.policy_period_edit_effective_date,
    tb_dim_policy.cancellation_date,
    tb_dim_policy.product_code, 
    tb_dim_policy.brand_code                         
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

