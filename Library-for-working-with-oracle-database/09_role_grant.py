
import cx_Oracle
import os

d_sql = "E://Develop/Python/Grant/SQL/"
d_cmd = "E://Develop/Python/Grant/CMD/"

try:

   connection = cx_Oracle.connect(                                                                   
       user="scott",
       password="tiger",
       dsn="odb")
       
   cur = connection.cursor()
   
   f_cmd = d_cmd+'role_grant.cmd'
   f_sql = d_cmd+'role_grant.sql'
   
   f_name = d_sql+'role_grant.sql'
   
   s_sql = f'''SET LONG 2000000
      SET PAGESIZE 0

      EXECUTE DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'STORAGE',false);

      spool {f_name}
      
      select (case 
	            when ((select count(*)
	               from   dba_role_privs
	               where  grantee = 'SCOTT') > 0)
	           then  dbms_metadata.get_granted_ddl ('ROLE_GRANT', 'SCOTT') 
	           else  to_clob ('   -- No granted Roles found !')
	        end ) from dual;      
      
      spool off;

      EXECUTE DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'DEFAULT');

      EXIT '''   

   
   f = open(f_sql,'w+')
   
   f.write(s_sql)
   
   f.close()   
   
   s_cmd = f'''sqlplus scott/tiger@odb @{f_sql}\n'''
         
   f = open(f_cmd,'a+')
   
   f.write(s_cmd)
   
   f.close()        
      
      
except cx_Oracle.DatabaseError as e:
    print("Problem with Oracle", e)        
        
finally:
    if cur:
       cur.close()
    if connection:
       connection.close()        
       
       