create or replace function dds.generate_script()
RETURNS void
LANGUAGE plpgsql
VOLATILE
as 
$$

declare 

col_names text[];
col_name  text;
line text;
sql_text text;

begin

	sql_text := 'select distinct r."Сервис-провайдер"
				, r."Код СП"
				, case when "Канал" = ''ЕК'' then ''ОР'' else "Каналя" end "Канал"
				, case when "Канал" = ''ЕКО'' then ''ЕКО'' end "МК"
				, null "Направление"
				, m."Уровень согласования"
				, m."Начисления"
				';

	
	
	select array_agg(column_name::text ORDER BY ordinal_position) from information_schema.columns
	into col_names
	where table_name = 'discount_matrixis'
	and table_schema = 'bi_an'
	and column_name like '%Новая скидка%';
	
	FOREACH col_name IN ARRAY col_names LOOP

 
		line := format(
                    ', case when m.%1$I not like ''%%.0000%%'' ' ||
                    'then regexp_replace(m.%1$I, ''0%%'', ''1%%'') ' ||
                    'else m.%1$I end %1$I',
                    col_name
                	);
		sql_text := sql_text || line || E'\n';

        -- тут ваша логика
    END LOOP;

	sql_text := sql_text || 'from bi_an.discount_matrixis m
							 left join bi_an.sp_regions r on r."Регион" = m."Регион"
							 order by "Сервис-провайдер";';
	
	RAISE NOTICE '%', sql_text;	
	
		
end;
$$
EXECUTE ON ANY;
