select table_name,
           column_name,
           data_type || ' (' || coalesce(numeric_precision::text, '') || ','  || coalesce(numeric_scale::text, 'безразмерно') || ')'  as data_type,
           case
                   when numeric_precision <= 5 and numeric_scale = 0 then 'int2'
                   when numeric_precision <= 10 and numeric_scale = 0 then 'int4'
                   when numeric_precision <= 19 and numeric_scale = 0 then 'int8'
           else 'эквивалентно текущему типу данных'
           end as numeric_to_int
from information_schema.columns
where table_schema = 'gl'
and   data_type = 'numeric'
and   table_name in (select trg_tbl_name from meta_info.ee_gl_md)
