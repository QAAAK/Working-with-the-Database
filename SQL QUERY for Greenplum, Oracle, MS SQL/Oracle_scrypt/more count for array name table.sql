select 'SELECT ''' || table_name || ''', COUNT(*) FROM ' || table_name || 'UNION'
where table_name in (SELECT table_name from ALL_TABLES WHERE [table_name])
