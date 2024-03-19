SELECT

t.Name AS TableName,

SUM(a.total_pages) * 8 AS TotalSpaceKB

FROM

sys.tables t

INNER JOIN sys.indexes i ON t.object_id = i.object_id

INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id

INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id

LEFT OUTER JOIN sys.schemas s ON t.schema_id = s.schema_id

WHERE

t.Name NOT LIKE 'dt%'

AND t.is_ms_shipped = 0

AND i.object_id > 255

GROUP BY

t.Name, s.Name, p.Rows

ORDER BY

t.Name;
