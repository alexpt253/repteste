CREATE OR REPLACE FUNCTION source.fn_load_tb_dim_policy_stg(IN batch_name text, IN batch_number integer, IN batch_from_timestamp timestamp without time zone, IN batch_to_timestamp timestamp without time zone

                                                           ,OUT out_function_status text, OUT out_function_error_message text)

RETURNS RECORD AS 

$BODY$



BEGIN



--the function should get batch name as input parameter and based on that the partition should be dropped

execute 'alter table source.tb_dim_policy_stg truncate partition ' || batch_name ;



INSERT INTO source.tb_dim_policy_stg ( 

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



WITH cte_MinEditEffectiveDate AS

( SELECT policynumber, MIN(EditEffectiveDate) as MinEditEffectiveDate

    FROM operational_source_pc.pc_policyperiod

    GROUP BY policynumber

)





SELECT CASE  WHEN pp.policynumber IS NULL THEN 'PC_AU' || '~' || 'Quote' || '~' || COALESCE(j.jobnumber,'') 

      ELSE 'PC_AU' || '~' || pp.policynumber 

      END::TEXT                                                      AS policy_business_key   

    , pp.policynumber                                           AS policy_number

    , pps_pctl.typecode                                         AS policy_status_code

    , pps_pctl.description                                      AS policy_status_desc

    , NULL                                                      AS policy_status_created_date  

    , cte_meed.MinEditEffectiveDate                             AS policy_inception_date   --Logic confirmed, v2

    , cte_meed.MinEditEffectiveDate                             AS original_policy_inception_date

    , CAST(pol.IssueDate AS DATE)                               AS issue_date

    , pp.termnumber                                             AS term_number      

    , pp.PeriodStart                                            AS term_start_date

    , pp.PeriodEnd                                              AS term_end_date

    , pp.modelnumber                                            AS model_number

    , pp.EditEffectiveDate                                      AS policy_period_edit_effective_date

    , pp.mostrecentmodel                                        AS most_recent_model

    , tt_pctl.typecode                                          AS term_type_code               --Logic confirmed, v2

    , tt_pctl.description                                       AS term_type_desc               --Logic confirmed, v2

    , pp.cancellationdate                                       AS cancellation_date

    , rc_pctl.typecode                                          AS cancellation_reason_code

    , rc_pctl.name                                              AS cancellation_reason_desc

    , pp.billingmethod                                          AS billing_method_code

    , bm_pctl.name                                              AS billing_method_desc

    , pp.primaryinsuredname                                     AS primary_insured_name

    , pro.code                                                  AS policy_producer_code

    , pro.description                                           AS policy_producer_desc

    , s.typecode                                                AS market_segment_code

    , s.description                                             AS market_segment_desc

    , pr_pctl.typecode                                          AS package_risk_code

    , pr_pctl.description                                       AS package_risk_desc

    , pol.productcode                                           AS product_code

    , jur_pctl.typecode                                         AS jurisdiction_code   

    , jur_pctl.description                                      AS jurisdiction_desc   

    , b_pctl.typecode                                           AS brand_code

    , b_pctl.description                                        AS brand_name

    , nrc_pctl.typecode                                         AS non_renewal_reason_code      

    , nrc_pctl.description                                      AS non_renewal_reason_desc      

    , CASE WHEN pp.editeffectivedate = pp.cancellationdate THEN COALESCE(cnt.FirstName,'') || ' ' || COALESCE(cnt.LastName ,'')

      END                                                       AS cancelled_by_name            

    , pp.retired                                                AS retired

    , pp.updatetime                                             AS md_row_effective_date

    , 'PC_AU'::TEXT                                                   AS md_source_system_code



FROM operational_source_pc.pc_policyperiod pp

INNER JOIN operational_source_pc.pc_policy pol ON pp.policyid=pol.id

LEFT OUTER JOIN operational_source_pc.pctl_policyperiodstatus pps_pctl ON pp.Status = pps_pctl.ID

LEFT OUTER JOIN operational_source_pc.pc_job j on pp.id = j.policyid 

LEFT OUTER JOIN cte_MinEditEffectiveDate cte_meed ON cte_meed.policynumber=pp.policynumber

LEFT OUTER JOIN operational_source_pc.pctl_termtype tt_pctl ON pp.selectedtermtype =tt_pctl.ID

LEFT OUTER JOIN operational_source_pc.pc_job j1 ON pp.JobID = j1.ID

LEFT OUTER JOIN operational_source_pc.pctl_reasoncode rc_pctl ON j1.CancelReasonCode = rc_pctl.ID

LEFT OUTER JOIN operational_source_pc.pc_producercode pro ON pol.producercodeofserviceid = pro.ID

LEFT OUTER JOIN operational_source_pc.pctl_billingmethod bm_pctl ON pp.BillingMethod = bm_pctl.ID

LEFT OUTER JOIN operational_source_pc.pctl_segment s ON pp.segment=s.id

LEFT OUTER JOIN operational_source_pc.pctl_packagerisk pr_pctl ON pol.packagerisk=pr_pctl.id

LEFT OUTER JOIN operational_source_pc.pctl_jurisdiction jur_pctl ON pp.BaseState=jur_pctl.id

LEFT OUTER JOIN operational_source_pc.pc_policyterm pt ON pp.policytermid  = pt.ID 

LEFT OUTER JOIN operational_source_pc.pctl_iag_brand b_pctl ON pt.brand = b_pctl.id

LEFT OUTER JOIN operational_source_pc.pc_policyterm pt2 ON pp.termnumber  = pt2.ID 

LEFT OUTER JOIN operational_source_pc.pc_job j3 ON pp.id = j3.policyid AND j3.policyterm=pt2.ID

LEFT OUTER JOIN operational_source_pc.pctl_NonRenewalCode nrc_pctl ON j3.nonrenewalcode = nrc_pctl.id

LEFT OUTER JOIN operational_source_pc.pc_user usr ON j3.CreateUserID=usr.ID

LEFT OUTER JOIN operational_source_pc.pc_contact cnt ON usr.ContactID=cnt.ID

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

