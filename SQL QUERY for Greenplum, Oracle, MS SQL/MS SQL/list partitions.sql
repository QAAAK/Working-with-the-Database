select distinct t.name, p.rows from sys.tables as t

inner join sys.partitions p on t.object_id = p.object_id

where t.type = 'U';
