
import cx_Oracle
import os

d_sql = "E://Develop/Python/View/SQL/"
d_cmd = "E://Develop/Python/View/CMD/"

try:

   connection = cx_Oracle.connect(                                                                   
       user="scott",
       password="tiger",
       dsn="odb")
       
   cur = connection.cursor()
   
   cur.execute("select * from scott.s1 where vid = 'vv'")

   for vid, name, fname in cur:
   
     v_name = name   
   
     f_cmd = d_cmd+'view.cmd'
     f_sql = d_cmd+fname+'.sql'
     
     f_name = d_sql+name+'.sql'
   
     s_sql = f'''SET LONG 2000000
        SET PAGESIZE 0

        EXECUTE DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'STORAGE',false);

        spool {f_name}
        select dbms_metadata.get_ddl('VIEW','{v_name}','SCOTT') from dual;
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
       
       