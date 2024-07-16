                                                                                
  CREATE TABLE "SCOTT"."EMP"                                                    
   (	"EMPNO" NUMBER(4,0),                                                       
	"ENAME" VARCHAR2(10),                                                          
	"JOB" VARCHAR2(9),                                                             
	"MGR" NUMBER(4,0),                                                             
	"HIREDATE" DATE,                                                               
	"SAL" NUMBER(7,2),                                                             
	"COMM" NUMBER(7,2),                                                            
	"DEPTNO" NUMBER(2,0),                                                          
	 CONSTRAINT "PK_EMP" PRIMARY KEY ("EMPNO")                                     
  USING INDEX PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS             
  TABLESPACE "USERS"  ENABLE                                                    
   ) SEGMENT CREATION IMMEDIATE                                                 
  PCTFREE 10 PCTUSED 0 INITRANS 1 MAXTRANS 255                                  
 NOCOMPRESS LOGGING                                                             
  TABLESPACE "USERS"                                                            
                                                                                

