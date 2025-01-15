select tc.table_schema, tc.table_name, kc.column_name
from
    information_schema.table_constraints tc,
    information_schema.key_column_usage kc
where
    tc.constraint_type = 'PRIMARY KEY'
    and kc.table_name = tc.table_name and kc.table_schema = tc.table_schema
    and kc.constraint_name = tc.constraint_name
    and tc.table_name not like '%_prt_%'


-- SELECT conrelid::regclass AS table_name, 
--  conname AS primary_key, 
--  pg_get_constraintdef(oid) 
-- FROM pg_constraint 
-- WHERE contype = 'p' 
-- AND connamespace = 'public'::regnamespace 
-- AND conrelid::regclass::text = ‘your_table_name’ 
-- ORDER BY conrelid::regclass::text, contype DESC;
