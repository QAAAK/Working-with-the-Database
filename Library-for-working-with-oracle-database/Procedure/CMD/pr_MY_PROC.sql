SET LONG 2000000
        SET PAGESIZE 0

        EXECUTE DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'STORAGE',false);

        spool E://Develop/Python/Procedure/SQL/MY_PROC.sql
        select dbms_metadata.get_ddl('PROCEDURE','MY_PROC','SCOTT') from dual;
        spool off;

        EXECUTE DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'DEFAULT');

        EXIT 