                                                                                
  CREATE TABLE "SCOTT"."EMP_TMP"                                                
   (	"EMPNO" NUMBER(4,0) NOT NULL ENABLE,                                       
	"ENAME" VARCHAR2(10),                                                          
	"JOB" VARCHAR2(9),                                                             
	"MGR" NUMBER(4,0),                                                             
	"HIREDATE" DATE,                                                               
	"SAL" NUMBER(7,2),                                                             
	"COMM" NUMBER(7,2),                                                            
	"DEPTNO" NUMBER(2,0)                                                           
   ) SEGMENT CREATION DEFERRED                                                  
  PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255                                 
 NOCOMPRESS LOGGING                                                             
  TABLESPACE "USERS"                                                            
                                                                                

