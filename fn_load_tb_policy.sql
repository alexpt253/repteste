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
  brand,
  policy_line_type,
  policy_term_start_date,
  policy_term_expiry_date,
  policy_term_effective_date,
  policy_term_commencement_date,
  policy_term_period,
  cancellation_date,
  exposure_start_date,
  exposure_end_date
)
with cte1 as (

  with cte2 as (

    with cte3 as (

      with cte4 as (
        SELECT a.md_source_system_code,
        a.policy_business_key,                 
        a.policy_number,  
        a.original_policy_inception_date, 
        a.term_number,
        a.term_start_date,                    
        a.term_end_date,                      
        a.policy_period_edit_effective_date,
        a.product_code, 
        a.brand_code,
        cast(a.cancellation_date as date) as cancellation_date,
        b.personal_motor_effective_date,  
        c.coverage_effective_date      
        coverage_effective_date, 
        coverage_expiration_date,
        personal_motor_expiry_date,
        rank() over(partition by a.policy_business_key order by coverage_effective_date, coverage_expiration_date, a.policy_business_key)
        FROM tb_dim_policy a 
        LEFT OUTER JOIN tb_dim_coverable_personal_motor b on a.policy_business_key=b.policy_business_key
        LEFT OUTER JOIN tb_dim_coverage_personal_motor c ON b.policy_business_key=c.policy_business_key
        WHERE a.md_row_status='A'
        AND b.md_row_status='A'
        AND c.md_row_status='A'
       ) 

      select rank, 
      max(rank) over(partition by policy_business_key) as max_rank, 
      md_source_system_code,
      policy_business_key,                 
      policy_number,  
      original_policy_inception_date, 
      term_number,
      term_start_date,                    
      term_end_date,                      
      policy_period_edit_effective_date,
      product_code, 
      brand_code,
      cancellation_date,
      personal_motor_effective_date,  
      coverage_effective_date      
      coverage_effective_date, 
      coverage_expiration_date,
      personal_motor_expiry_date,
      lead(coverage_expiration_date,1) over(order by rank) as next_row_coverage_expiration_date
      from cte4
    )

    select rank,
    max_rank,
    md_source_system_code,
    policy_business_key,                 
    policy_number,  
    original_policy_inception_date, 
    term_number,
    term_start_date,                    
    term_end_date,                      
    policy_period_edit_effective_date,
    product_code, 
    brand_code,
    cancellation_date,
    personal_motor_effective_date,  
    coverage_effective_date      
    coverage_effective_date, 
    coverage_expiration_date,
    next_row_coverage_expiration_date,
    personal_motor_expiry_date, 
    case when rank < max_rank then 
      case when coverage_expiration_date< next_row_coverage_expiration_date then
          coverage_expiration_date
      else
          coverage_effective_date
      end
    else
      least(term_end_date,personal_motor_expiry_date, coverage_expiration_date)
    end as exposure_end_date
    from cte3

  )
  select  rank,
  max_rank,
  md_source_system_code,
  policy_business_key,                 
  policy_number,  
  original_policy_inception_date, 
  term_number,
  term_start_date,                    
  term_end_date,                      
  policy_period_edit_effective_date,
  product_code, 
  brand_code,
  cancellation_date,
  personal_motor_effective_date,  
  coverage_effective_date      
  coverage_effective_date, 
  coverage_expiration_date,
  next_row_coverage_expiration_date,
  personal_motor_expiry_date,
  lag(exposure_end_date,1) over(order by rank) as previous_row_exposure_end_date,
  exposure_end_date
  from cte2

)


select --rank,
  --max_rank,
  md_source_system_code,
  policy_business_key,                 
  policy_number,  
  brand_code,
  product_code, 
  term_start_date,                    
  term_end_date,                      
  policy_period_edit_effective_date,
  original_policy_inception_date, 
  term_number,
  cancellation_date,
  --personal_motor_effective_date,  
  --next_row_coverage_expiration_date,
  --personal_motor_expiry_date,
  --coverage_effective_date, 
  --coverage_expiration_date,
  case when rank=1 then 
        greatest(term_start_date,personal_motor_effective_date,coverage_effective_date) 
  else
        case when greatest(term_start_date,personal_motor_effective_date,coverage_effective_date) < previous_row_exposure_end_date then
            previous_row_exposure_end_date
        else
            greatest(term_start_date,personal_motor_effective_date,coverage_effective_date)
        end
  end as exposure_start_date,
  --greatest(term_start_date,personal_motor_effective_date,coverage_effective_date) as max_date,
  exposure_end_date--,
  --previous_row_exposure_end_date
from cte1;


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

