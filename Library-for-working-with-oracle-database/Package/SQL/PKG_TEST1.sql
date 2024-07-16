                                                                                
  CREATE OR REPLACE EDITIONABLE PACKAGE "SCOTT"."PKG_TEST1"                     
 as                                                                             
     function getArea (i_rad NUMBER) return NUMBER;                             
     procedure p_print (i_str1 VARCHAR2 :='hello',                              
                        i_str2 VARCHAR2 :='world',                              
                        i_end VARCHAR2  :='!' );                                
 end;                                                                           
                                                                                
CREATE OR REPLACE EDITIONABLE PACKAGE BODY "SCOTT"."PKG_TEST1"                  
 as                                                                             
     function getArea (i_rad NUMBER)return NUMBER                               
     is                                                                         
         v_pi NUMBER:=3.14;                                                     
     begin                                                                      
        return v_pi * (i_rad ** 2);                                             
     end;                                                                       
                                                                                
     procedure p_print(i_str1 VARCHAR2 :='hello',                               
                       i_str2 VARCHAR2 :='world',                               
                       i_end VARCHAR2  :='!' )                                  
     is                                                                         
     begin                                                                      
         DBMS_OUTPUT.put_line(i_str1||','||i_str2||i_end);                      
     end;                                                                       
 end;                                                                           
                                                                                

