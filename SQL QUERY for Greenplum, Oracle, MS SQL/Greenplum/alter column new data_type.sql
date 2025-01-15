select 'ALTER TABLE stg.' || table_name || ' ALTER COLUMN "w$loadid" TYPE varchar(56) USING "w$loadid"::varchar;'  from information_schema.columns
where table_schema = 'stg' and column_name = 'w$loadid' and data_type = 'text'
and table_name not like  '%_prt_%' and table_name in (select stg_tbl_name from meta_info.ee_stg_md)
