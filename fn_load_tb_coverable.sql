CREATE OR REPLACE FUNCTION summarization.fn_load_tb_coverable(IN batch_name text, IN batch_number integer, IN batch_from_timestamp timestamp without time zone, IN batch_to_timestamp timestamp without time zone

                                                           ,OUT out_function_status text, OUT out_function_error_message text)

RETURNS RECORD AS 

$BODY$



BEGIN



--the function should get batch name as input parameter and based on that the partition should be dropped

-- execute 'alter table summarization.tb_policy_stg truncate partition ' || batch_name ;


INSERT INTO tb_coverable 
(policy_line_id,
 coverable_id,
 external_vehicle_code,
 vehicle_make,
 vehicle_body_type,
 agreed_value,
 vehicle_garaged,
 vehicle_garaged_description,
 vehicle_use,
 coverable_effective_date,
 coverable_expiry_date)

SELECT
    tb_dim_policy_line.policy_line_business_key                             AS policy_line_id,
    tb_dim_coverable_personal_motor.coverable_personal_motor_business_key   AS coverable_id,
    NULL AS external_vehicle_code,
    tb_dim_coverable_personal_motor.vehicle_make,
    tb_dim_coverable_personal_motor.vehicle_body_type_code                  AS verycle_body_type,
    tb_dim_coverable_personal_motor.vehicle_agreed_value_amount             AS agreed_value,
    tb_dim_coverable_personal_motor.vehicle_car_park_type_code              AS vericle_garaged,
    tb_dim_coverable_personal_motor.vehicle_car_park_type_desc              AS vericle_garaged_desc,
    tb_dim_coverable_personal_motor.vehicle_primary_use_desc                AS vehicle_use,
    tb_dim_coverable_personal_motor.personal_motor_effective_date           AS coverable_effective_date,
    tb_dim_coverable_personal_motor.personal_motor_expiry_date              AS coverable_expiration_date
FROM
    tb_dim_coverable_personal_motor
    LEFT OUTER JOIN tb_dim_policy_line ON tb_dim_coverable_personal_motor.policy_business_key = tb_dim_policy_line.policy_business_key;


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

