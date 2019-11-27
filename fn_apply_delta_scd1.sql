--liquibase formatted sql
--changeset sr:s116038 endDelimiter:# splitStatements:false
--comment: Create generic function to apply delta changes for all dimensiontables. 
-- ----------------------------------------------------------------------------------------------------
-- Author     : Snigdha Rajput
-- Description: Initial version of the function for applying the delta changes to the target tables in integration layer
-- Inputs     : batch_name, batch_number, batch_from_timestamp, batch_to_timestamp
--            : Input table : source table in layer A and target table in layer B
-- Outputs    : out_function_status, out_function_error_message
-- ----------------------------------------------------------------------------------------------------
-- VERSIONS  DATE          WHO                  DESCRIPTION
-- 1.00      21-10-2019    Snigdha Rajput      Initial Version of the file
-- 2.00      21-11-2019    Snigdha Rajput	   Added the tag "sr:s116038 splitStatements:false" in the header section 
-- 3.00      25-11-2019    Snigdha Rajput       Added the "endDelimiter:#" in the beginning and # at the end

-----------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION fn_apply_delta_scd1(IN batch_name text ,IN batch_number integer , IN source_table_name text,IN target_table_name text, OUT out_function_status text, OUT out_function_error_message text)
	RETURNS record
	LANGUAGE plpgsql
	VOLATILE
AS $function$ 	

DECLARE

v_hash_col_list    text;
v_insert_col_list  text;
v_select_col_list  text;
business_key      text  := ltrim(target_table_name,'tb_dim_' ) || '_business_key';
high_end_date    varchar(20) := '9999-12-31 23:59:59';
active_status	 varchar(1) := 'A';
schema_name  text := 'public';  

BEGIN

--delete from table delta action 
delete from delta_action_scd1 
where table_name = target_table_name
and batch_name = batch_name;

-- column list for hash function

select rtrim(string_agg( 'coalesce(' || column_name || '::TEXT, ''~'')' || ' || '  order by column_name) , ' || ') into v_hash_col_list
from information_schema.columns
where table_schema = 'public'        --layer a schema name 'source'
and   table_name = source_table_name    --staging version of the table 'tb_dim_account'
and column_name <> 'md_row_effective_date'
group by table_name;

-- column list for insert statement
select rtrim(string_agg( column_name || ','  order by column_name) , ',') into v_insert_col_list
from information_schema.columns
where table_schema = 'public'          --layer A schema name  'source'
and   table_name = source_table_name   --staging table in layer A 'tb_dim_account_stg'
group by table_name;



execute    'INSERT INTO delta_action_scd1 '      
		||'  SELECT src.src_business_key ' 
		||'  ,src.src_update_time  '
		|| ' ,case when tgt.tgt_business_key is null then ''INSERT'''
		|| '  	   when tgt.tgt_business_key is not null and src.src_hash_key <> tgt.tgt_hash_key then ''UPDATE'''
		|| ' 	   when tgt.tgt_business_key is not null and src.src_hash_key = tgt.tgt_hash_key then ''NO ACTION'''
		|| ' else NULL '
		|| ' end as delta_action'
		|| ' ,'''|| target_table_name ||''',''' ||  batch_name || '''' 
		|| ' from ( select '|| business_key || ' as  src_business_key , md_row_effective_date as src_update_time , md5 ( ' || v_hash_col_list || ') as src_hash_key from '|| source_table_name	|| ' ) AS src  '  
		|| ' left outer join '
		|| '( select ' || business_key || ' as tgt_business_key , md5(' || v_hash_col_list || ') as tgt_hash_key' || ' from ' || target_table_name		|| '  where md_row_status = ''A'') as tgt '
		|| ' on src.src_business_key = tgt.tgt_business_key';
	


-- column list for insert function

select rtrim(string_agg( 's.' || column_name || ','  order by column_name) , ',') into v_select_col_list
from information_schema.columns
where table_schema = 'public'        --layer A schema name  'source'
and   table_name = source_table_name   --staging table in layer A 'tb_dim_account_stg'
group by table_name;

execute 'insert into '   || target_table_name || ' ( ' || v_insert_col_list 
		|| ', md_row_expiry_date, md_row_status, md_row_insert_audit_key, md_row_update_audit_key, md_row_process_insert_timestamp, md_row_process_update_timestamp )'
		|| ' Select ' || v_select_col_list || ',' 
		||  quote_literal(high_end_date)|| '::timestamp, ' 
		|| 'case when coalesce(s.retired,0) <> 0 Then  ''D''	else ''A'' end ,'		
		||   batch_number || ',' || batch_number || ', now()::timestamp, now()::timestamp '
         	|| ' from public.'	|| source_table_name
		||' s inner join delta_action_scd1 t on s.' || business_key || ' = t.business_key  and s.md_row_effective_date = t.update_time '
		|| ' and t.delta_action in (''INSERT'',''UPDATE'')'
		|| ' and t.batch_name = ''' || batch_name || ''''
		|| ' and t.table_name = ''' || target_table_name || ''''
		 ;



				execute 'create table ' || 'Temp_' || batch_name ||'_' || target_table_name 
				||' as 	Select ' 
				|| business_key 
				|| ', md_row_effective_date, md_row_update_audit_key,	(lead(md_row_effective_date) over (partition by ' 
				|| business_key || ' order by md_row_effective_date)) - interval '||quote_literal('00:00:00.005') ||' as md_row_expiry_date_calc			
				From  ' || target_table_name || ' 	
				where md_row_expiry_date = ' || quote_literal(high_end_date)|| '::timestamp
				distributed by (' || business_key|| ')';


				execute 'Update ' || schema_name || '.' || target_table_name || ' target set				
				md_row_expiry_date =  md_row_expiry_date_calc	,		
				md_row_process_update_timestamp = now()	,		
				md_row_status =  '||quote_literal('I') ||'			,
				md_row_update_audit_key = '|| batch_number ||'			


				From ' || schema_name || '.Temp_' ||  batch_name ||'_' || target_table_name ||
				' 	tem  Where target.' || business_key || ' = tem.' || business_key ||		-- should be updated with the business_id column which would be common in all structures		
				' and target.md_row_effective_date = tem.md_row_effective_date	and tem.md_row_expiry_date_calc is not NULL'	;			

				execute 'drop table ' || schema_name || '.Temp_' || batch_name ||'_' || target_table_name;


out_function_status := 'SUCCESS';
return;

exception
 when others then
    out_function_status := 'FAIL';
	out_function_error_message := SQLSTATE||': '||SQLERRM;
	return;

END


 $function$
;
#
