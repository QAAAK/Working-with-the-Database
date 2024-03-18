SELECT 'ALTER TABLE schema_name.'||table_name||' DROP CONSTRAINT '||constraint_name||';'

FROM information_schema.constraint_table_usage

where table_schema = 'stg';