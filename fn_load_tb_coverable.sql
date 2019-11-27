CREATE OR REPLACE FUNCTION summarization.fn_load_tb_coverable(IN batch_name text, IN batch_number integer, IN batch_from_timestamp timestamp without time zone, IN batch_to_timestamp timestamp without time zone

                                                           ,OUT out_function_status text, OUT out_function_error_message text)

RETURNS RECORD AS 

$BODY$



BEGIN



--the function should get batch name as input parameter and based on that the partition should be dropped

-- execute 'alter table summarization.tb_policy_stg truncate partition ' || batch_name ;




SELECT 

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

