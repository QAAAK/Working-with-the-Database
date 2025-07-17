CREATE OR REPLACE FUNCTION dds.update_product_budget_detail(month date)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
			
begin 
	
	set role analyze_bi_owner;
    
    EXECUTE 'UPDATE dds.product_report a
	SET product_budget_detail = p.product_budget_detail
	FROM (
	  SELECT *, ROW_NUMBER() OVER(PARTITION BY group_id, sub_acc_trans) as rn
	  FROM cvm_b2b.vas_iot_product_title_dict_rdm
	  WHERE deleted = 0 AND d_to IS NULL
	) p
	WHERE a.group_id = p.group_id
	AND a.table_business_month = ''' || month || '''
	AND a.sub_acc_trans = p.sub_acc_trans
	AND p.rn = 1;';

return 'passed';
	
end;



$$
EXECUTE ON ANY;
