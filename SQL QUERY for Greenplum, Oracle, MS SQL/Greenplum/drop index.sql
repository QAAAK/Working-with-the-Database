SELECT

tablename,

'DROP INDEX schema_name.' || indexname || ';'

FROM

pg_indexes

WHERE

schemaname = 'stg'

and indexname like 'uk%'

ORDER BY

tablename,

indexname;
