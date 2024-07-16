
import cx_Oracle

try:

   connection = cx_Oracle.connect(                                                                   
       user="scott",
       password="tiger",
       dsn="odb")

   print("Connected to Oracle Database")
   
   cursor = connection.cursor()
   
   # Fill a table  S1
   
   cursor.execute("""
       begin
       
           execute immediate 'truncate table s1';
                  
           INSERT INTO s1
                select 'tt', table_name, 'tt_' || table_name from USER_TABLES where table_name <> 'S1';
               
           INSERT INTO s1
                SELECT 'ii', index_name, 'ii_' || index_name from  user_indexes;     
                
           INSERT INTO s1      
                SELECT 'vv', view_name,  'vv_' || view_name from USER_VIEWS;     
                
           INSERT INTO s1     
           
           SELECT 'ff', object_name, 'ff_' || object_name
             FROM dba_objects
           WHERE owner LIKE 'SCOTT'
             AND object_type IN ('FUNCTION');      
             
                INSERT INTO s1     
           
           SELECT 'pr', object_name, 'pr_' || object_name
             FROM dba_objects
           WHERE owner LIKE 'SCOTT'
             AND object_type IN ('PROCEDURE');  
           
               INSERT INTO s1     
           
           SELECT 'pk', object_name, 'pk_' || object_name
             FROM dba_objects
           WHERE owner LIKE 'SCOTT'
             AND object_type IN ('PACKAGE');      
                      
                     
           COMMIT;
          
       end;""")

            
except cx_Oracle.DatabaseError as e:
    print("Problem with Oracle", e)        
        
finally:
    if cursor:
       cursor.close()
    if connection:
       connection.close()        
        
        


        