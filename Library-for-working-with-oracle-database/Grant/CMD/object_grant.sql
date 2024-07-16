SET LONG 2000000
      SET PAGESIZE 0

      EXECUTE DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'STORAGE',false);

      spool E://Develop/Python/Grant/SQL/object_grant.sql
      
      select (case 
	       when ((select count(*)
	              from   dba_tab_privs
	              where  grantee = 'SCOTT') > 0)
	       then  dbms_metadata.get_granted_ddl ('OBJECT_GRANT', 'SCOTT') 
	       else  to_clob ('   -- No Object Privileges found !')
	       end ) from dual;   
      
      spool off;

      EXECUTE DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'DEFAULT');

      EXIT 