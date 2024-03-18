SELECT nspname, proname

FROM pg_catalog.pg_namespace

JOIN pg_catalog.pg_proc

ON pronamespace = pg_namespace.oid

ORDER BY Proname