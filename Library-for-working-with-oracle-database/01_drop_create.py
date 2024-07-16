
import cx_Oracle

try:

   connection = cx_Oracle.connect(                                                                   
       user="scott",
       password="tiger",
       dsn="odb")

   print("Connected to Oracle Database")
   
   cursor = connection.cursor()
   
   # Create a table
   
   cursor.execute("""
       begin
          execute immediate 'drop table scott.s1';
          exception when others then if sqlcode <> -942 then raise; end if;
       end;""")

   cursor.execute("""
       create table scott.s1 (
                              vid VARCHAR2(2),
                              name VARCHAR2(50),
                              fname VARCHAR2(50))""")
   
            
except cx_Oracle.DatabaseError as e:
    print("Problem with Oracle", e)        
        
finally:
    if cursor:
       cursor.close()
    if connection:
       connection.close()        
        
        


        