CREATE OR REPLACE PROCEDURE "P_DM_BRANCH"
(IO_STATUS OUT INTEGER,IO_SQLERR OUT VARCHAR2)
AS

	TABLE_NAME VARCHAR2(125) :='DM_BRANCH';  -- 表名(修改)
  PROCEDURE_NAME VARCHAR2(125) :='P_'||TABLE_NAME;  -- 存储过程名(修改)

  IO_ROW INTEGER;  --插入条数
  ST_ROW INTEGER;  --源数据条数

  V_ETL_DATE NUMBER;  -- 跑批日期
  V_START_TIMESTAMP TIMESTAMP;  -- 加载开始时间
  V_END_TIMESTAMP TIMESTAMP;  -- 加载结束时间
  
  DM_TODAY NUMBER;        -- 数据日期"当日"
	TX_DATE NUMBER;

  DM_SQL VARCHAR2(20000);  -- 存放SQL语句

BEGIN

  SELECT SYSDATE INTO V_START_TIMESTAMP  FROM dual;  -- 加载程序运行开始时间

  SELECT TO_NUMBER(TO_CHAR((SYSDATE),'YYYYMMDD')) INTO V_ETL_DATE FROM dual;  -- 取系统日期作为跑批日期
  
  SELECT TX_DATE INTO DM_TODAY FROM MMAPST.ST_SYSTEM_DATE;  --取数据日期
  
  --查询ST层表中是否有'当日'数据
  SELECT COUNT(1) INTO ST_ROW FROM MMAPST.ST_BRANCH WHERE ETL_DATE=DM_TODAY;
  IF ST_ROW>0
  THEN
    --本调度逻辑为全删全插
    DM_SQL :='TRUNCATE TABLE MMAPDM.DM_BRANCH' ;   -- 删除表中数据
    EXECUTE IMMEDIATE DM_SQL;
    COMMIT;

    DM_SQL :='
     INSERT INTO MMAPDM.DM_BRANCH     
    (                              
      ETL_DATE                     
     ,REGION_ID                    
     ,REGION_NM                    
     ,FBRNH_ID                     
     ,FBRNH_NM                     
     ,BRNH_ID                      
     ,BRNH_NM                      
     ,EQUIPMENT_ID                 
     ,CHANNEL                      
     ,TELEPHONE                    
     ,ADDR                         
     ,SOURCEGRP                    
   )                               
    SELECT                         
      '||V_ETL_DATE||' AS ETL_DATE 
     ,REGION_ID                    
     ,REGION_NM                    
     ,FBRNH_ID                     
     ,FBRNH_NM                     
     ,BRNH_ID                      
     ,BRNH_NM                      
     ,EQUIPMENT_ID                 
     ,CHANNEL                      
     ,TELEPHONE                    
     ,ADDR 
     ,SOURCEGRP                        
 FROM MMAPST.ST_BRANCH';
    
    EXECUTE   IMMEDIATE DM_SQL;
    COMMIT;
    
    IO_ROW := SQL%ROWCOUNT ;  
    
  END IF;
  SELECT SYSDATE INTO V_END_TIMESTAMP FROM DUAL;  -- 加载程序运行结束时间

  IO_STATUS := 0 ;
  IO_SQLERR := 'SUSSCESS';
  P_MMAPDM_WRITE_LOGS(PROCEDURE_NAME,IO_STATUS,IO_ROW,V_START_TIMESTAMP,V_END_TIMESTAMP,IO_SQLERR);
  COMMIT;
  EXCEPTION
    WHEN OTHERS  THEN
  ROLLBACK;
  IO_STATUS := 9;
  IO_SQLERR := SQLCODE ||  SQLERRM;

  SELECT SYSDATE INTO V_END_TIMESTAMP FROM DUAL;
  P_MMAPDM_WRITE_LOGS(PROCEDURE_NAME,IO_STATUS,IO_ROW,V_START_TIMESTAMP,V_END_TIMESTAMP,IO_SQLERR);
END P_DM_BRANCH;
/
