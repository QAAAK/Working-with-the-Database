-- SELECT  obj.name AS FK_NAME,
--     sch.name AS [schema_name],
--     tab1.name AS [table],
--     col1.name AS [column],
--     tab2.name AS [referenced_table],
--     col2.name AS [referenced_column]
-- FROM sys.foreign_key_columns fkc
-- INNER JOIN sys.objects obj
--     ON obj.object_id = fkc.constraint_object_id
-- INNER JOIN sys.tables tab1
--     ON tab1.object_id = fkc.parent_object_id
-- INNER JOIN sys.schemas sch
--     ON tab1.schema_id = sch.schema_id
-- INNER JOIN sys.columns col1
--     ON col1.column_id = parent_column_id AND col1.object_id = tab1.object_id
-- INNER JOIN sys.tables tab2
--     ON tab2.object_id = fkc.referenced_object_id
-- INNER JOIN sys.columns col2
--     ON col2.column_id = referenced_column_id AND col2.object_id = tab2.object_id


SELECT F.NAME as 'Foreign key constraint name',
OBJECT_NAME(F.parent_object_id) AS 'Referencing/Child Table',
COL_NAME(FC.parent_object_id, FC.parent_column_id) AS 'Referencing/Child Column',
OBJECT_NAME(FC.referenced_object_id) AS 'Referenced/Parent Table',
COL_NAME(FC.referenced_object_id, FC.referenced_column_id) AS 'Referenced/Parent Column'
FROM sys.foreign_keys AS F
INNER JOIN sys.foreign_key_columns AS FC
ON F.OBJECT_ID = FC.constraint_object_id
--WHERE OBJECT_NAME (F.referenced_object_id) = 'table_name'
ORDER BY OBJECT_NAME(F.parent_object_id)

