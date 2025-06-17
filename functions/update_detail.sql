-- DROP FUNCTION dds.update_detail(date);

CREATE OR REPLACE FUNCTION dds.update_detail(month date)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
		
begin 
	
	EXECUTE 'DROP TABLE IF EXISTS temp_detail';
    
    EXECUTE 'CREATE TEMPORARY TABLE temp_detail
                               WITH (appendonly=true, compresstype=zstd, compresslevel=1)
                               ON COMMIT PRESERVE ROWS
                               AS (
          SELECT a.* FROM dds.odin_hive_product_report a 
          WHERE table_business_month =  ''' || month || ''') 
         WITH DATA DISTRIBUTED RANDOMLY';
    
    EXECUTE 'ANALYZE temp_detail';
    
    EXECUTE 'INSERT INTO dds.detail 
        SELECT a.*,t.tp_group_big   FROM temp_detail a 
        LEFT JOIN dds.guide_tp_group_big t ON a.tp_group1 = t.tp_group';

return 'passed';
	
end;



$$
EXECUTE ON ANY;

-- Permissions

ALTER FUNCTION dds.update_detail(date) OWNER TO analyze_bi_owner;
GRANT ALL ON FUNCTION dds.update_detail(date) TO public;
GRANT ALL ON FUNCTION dds.update_detail(date) TO analyze_bi_owner;



DO
$$
DECLARE
  month date;
BEGIN
  FOR month IN SELECT generate_series('2023-02-01'::date, '2025-06-01'::date, '1 month') 
  LOOP
	
	perform dds.update_detail(month);
	
	raise notice 'месяц % загружен', month;
  END LOOP;
END;
$$ LANGUAGE plpgsql;
