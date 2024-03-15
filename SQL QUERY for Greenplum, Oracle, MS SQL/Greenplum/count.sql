SELECT relname, n_live_tup
FROM pg_stat_user_tables
where schemaname != 'pg_catalog' and schemaname != 'gp_toolkit' and schemaname != 'information_schema' and schemaname != 'pg_aoseg' and table_name not like '%_prt_%';