CREATE OR REPLACE FUNCTION summarization.fn_load_tb_policy_stg(IN batch_name text, IN batch_number integer, IN batch_from_timestamp timestamp without time zone, IN batch_to_timestamp timestamp without time zone
                                                               ,OUT out_function_status text, OUT out_function_error_message text)


RETURNS RECORD AS 
$BODY$

BEGIN


--the function should get batch name as input parameter and based on that the partition should be dropped
execute 'alter table summarization.tb_policy_stg truncate partition ' || batch_name ;



INSERT INTO summarization.tb_policy_stg ( 
     policy_business_key
   , policy_number
    , policy_status_code
    , policy_status_desc
    , policy_status_created_date
    , policy_inception_date
    , original_policy_inception_date
    , issue_date
    , term_number
    , term_start_date
    , term_end_date
    , model_number
    , policy_period_edit_effective_date
    , most_recent_model
    , term_type_code
    , term_type_desc
    , cancellation_date
    , cancellation_reason_code
    , cancellation_reason_desc
    , billing_method_code
    , billing_method_desc
    , primary_insured_name
    , policy_producer_code
    , policy_producer_desc
   , market_segment_code
    , market_segment_desc
    , package_risk_code
    , package_risk_desc
    , product_code
    , jurisdiction_code
    , jurisdiction_desc
    , brand_code
    , brand_name
    , non_renewal_reason_code
    , non_renewal_reason_desc
    , cancelled_by_name
    , retired
    , md_row_effective_date
    , md_source_system_code
)


SELECT 
WHERE pp.updatetime >= batch_from_timestamp AND   pp.updatetime <= batch_to_timestamp;


out_function_status := 'SUCCESS';

RETURN;

EXCEPTION
  WHEN OTHERS THEN
    out_function_status := 'FAIL';
      out_function_error_message := SQLSTATE||': '||SQLERRM;
      RETURN;

END $BODY$

LANGUAGE plpgsql VOLATILE SECURITY INVOKER;

