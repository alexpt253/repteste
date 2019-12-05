CREATE OR REPLACE FUNCTION fn_load_tb_policy_period(OUT out_function_status text, OUT out_function_error_message text)


RETURNS RECORD AS 
$BODY$

BEGIN

truncate table tb_policy_period;

INSERT INTO tb_policy_period ( 
policy_term_number,
coverable_id,
coverage_id,
policy_period_start,
policy_period_end,
flag_active_row 
)
SELECT



out_function_status := 'SUCCESS';

RETURN;

EXCEPTION
  WHEN OTHERS THEN
    out_function_status := 'FAIL';
      out_function_error_message := SQLSTATE||': '||SQLERRM;
      RETURN;

END $BODY$

LANGUAGE plpgsql VOLATILE SECURITY INVOKER;
