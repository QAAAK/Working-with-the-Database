                                                                                
  CREATE OR REPLACE FORCE EDITIONABLE VIEW "SCOTT"."L_EMP" ("L", "EMPLOYEE") AS 
                                                                                
  SELECT LEVEL as l,                                                            
  LPAD(' ', 2 * LEVEL - 1) || ename || ' ' ||                                   
  job AS employee                                                               
 FROM emp                                                                       
 START WITH empno = 7839                                                        
 CONNECT BY PRIOR empno = mgr                                                   
                                                                                

