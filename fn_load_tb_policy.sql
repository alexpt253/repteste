CREATE OR REPLACE FUNCTION summarization.fn_load_tb_policy (IN batch_name text , IN batch_number integer

                                                , IN batch_from_timestamp timestamp without time zone, IN batch_to_timestamp timestamp without time zone

                                                , OUT out_function_status text, OUT out_function_error_message text)

RETURNS record AS

$BODY$

DECLARE



--source_table_name VARCHAR(50) := 'integration_dim_policy';

--target_table_name VARCHAR(50) := 'tb_policy';



function_return_results RECORD;

return_status text;

return_error_message text;



BEGIN





--call the generic function fn_apply_delta by passing source and target tables as parameters

--execute 'select  out_function_status, out_function_error_message from summarization.fn_apply_delta_scd2(''' || batch_name || ''',' || batch_number || ',''' ||source_table_name  || ''','''  || target_table_name ||    ''')' 

--          into function_return_results;

INSERT INTO public.integration_dim_policy 
(
  
  
)
SELECT         

return_status := function_return_results.out_function_status;

return_error_message := function_return_results.out_function_error_message;



if (return_status = 'FAIL')

then

    out_function_status := 'FAIL';

    out_function_error_message := return_error_message ;

    return; 

end if ;



out_function_status := 'SUCCESS';

return;



exception

 when others then

    out_function_status := 'FAIL';

    out_function_error_message := SQLSTATE||': '||SQLERRM;

    return;



END



$BODY$

LANGUAGE plpgsql VOLATILE SECURITY INVOKER;
