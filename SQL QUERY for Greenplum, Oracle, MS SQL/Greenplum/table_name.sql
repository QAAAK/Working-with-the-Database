
 /* SELECT table_name
  FROM information_schema.tables */

SELECT table_name
  FROM information_schema.tables
 where table_schema != 'pg_catalog' and table_schema != 'gp_toolkit' and table_schema != 'information_schema' and table_name not like '%_prt_%';