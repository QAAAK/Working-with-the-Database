SELECT    [TableName] = OBJECT_NAME(object_id),
last_user_update, last_user_seek, last_user_scan, last_user_lookup
FROM    sys.dm_db_index_usage_stats

-- SELECT name, STATS_DATE(object_id, stats_id)
-- FROM sys.stats 