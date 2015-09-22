CREATE OR REPLACE PROCEDURE P_DM_CUST_STATIC_IND
(IO_STATUS OUT INTEGER,IO_SQLERR OUT VARCHAR2)
AS


  TABLE_NAME VARCHAR2(125) :='DM_CUST_STATIC_IND';  -- 表名(修改)
  PROCEDURE_NAME VARCHAR2(125) :='P_'||TABLE_NAME;  -- 存储过程名(修改)

  IO_ROW INTEGER :=0 ;  --插入条数
  ST_ROW INTEGER;  --源数据条数

  V_START_TIMESTAMP TIMESTAMP;  -- 加载开始时间
  V_END_TIMESTAMP TIMESTAMP;  -- 加载结束时间

  DM_TODAY NUMBER;        -- 数据日期"当日"

  DM_SQL VARCHAR2(20000);
  V_COUNT NUMBER;  --计数变量
  V_COMMITNUM CONSTANT NUMBER :=500000;--一次提交记录数（默认一百万）
    --定义一个游标数据类型  
  TYPE emp_cursor_type IS REF CURSOR;  
  --声明一个游标变量  
  c1 EMP_CURSOR_TYPE;  
    --声明记录变量  
  V_DM_CUST_STATIC_IND DM_CUST_STATIC_IND%ROWTYPE;

BEGIN
  SELECT SYSDATE INTO V_START_TIMESTAMP  FROM dual;  -- 加载程序运行开始时间  
  SELECT TX_DATE INTO DM_TODAY FROM MMAPST.ST_SYSTEM_DATE;  -- 取数据日期
  -----建立临时表-----
  SELECT COUNT(1) INTO ST_ROW FROM USER_TABLES WHERE TABLE_NAME = 'TMP_CUST_CONTACT_RULE';
  IF ST_ROW >0
  THEN
      DM_SQL := 'DROP TABLE MMAPDM.TMP_CUST_CONTACT_RULE';
      EXECUTE  IMMEDIATE DM_SQL;
  END IF;
  
  DM_SQL:= 'CREATE TABLE MMAPDM.TMP_CUST_CONTACT_RULE AS
      SELECT 
          A.CUSTOMER_ID
          ,sum(case when B.ACPT_TYPE_CODE=''ATM'' then to_number(ACPT_IND) else 0 end)  AS ACPT_ATM_IND 
          ,sum(case when B.ACPT_TYPE_CODE=''DM'' then to_number(ACPT_IND) else 0 end)   AS ACPT_DM_IND  
          ,sum(case when B.ACPT_TYPE_CODE=''EDM'' then to_number(ACPT_IND) else 0 end)  AS ACPT_EDM_IND  
          ,sum(case when B.ACPT_TYPE_CODE=''MB'' then to_number(ACPT_IND) else 0 end)  AS ACPT_MB_IND  
          ,sum(case when B.ACPT_TYPE_CODE=''NB'' then to_number(ACPT_IND) else 0 end)  AS ACPT_NB_IND  
          ,sum(case when B.ACPT_TYPE_CODE=''PERSON'' then to_number(ACPT_IND) else 0 end)  AS ACPT_PERSON_IND  
          ,sum(case when B.ACPT_TYPE_CODE=''SMS'' then to_number(ACPT_IND) else 0 end)  AS ACPT_SMS_IND  
          ,sum(case when B.ACPT_TYPE_CODE=''WEBATM'' then to_number(ACPT_IND) else 0 end)  AS ACPT_WEBATM_IND  
          ,sum(case when B.ACPT_TYPE_CODE=''WECHAT'' then to_number(ACPT_IND) else 0 end)  AS ACPT_WECHAT_IND
      FROM MMAPST.ST_CUSTOMER A
      LEFT JOIN MMAPST.ST_CUST_CONTACT_RULE B
      ON A.CUSTOMER_ID = B.CUSTOMER_ID
      GROUP BY A.CUSTOMER_ID
  ';
  EXECUTE IMMEDIATE DM_SQL;
  COMMIT;
  
  SELECT COUNT(1) INTO ST_ROW FROM USER_TABLES WHERE TABLE_NAME = 'TMP_CUST_AGREEMENT';
  IF ST_ROW >0
  THEN
      DM_SQL := 'DROP TABLE MMAPDM.TMP_CUST_AGREEMENT';
      EXECUTE  IMMEDIATE DM_SQL;
  END IF;
  
  DM_SQL:= '
  CREATE TABLE MMAPDM.TMP_CUST_AGREEMENT AS
  SELECT 
      A.CUSTOMER_ID
      ,case when sum(case when C.AGMT_TYPE_CODE=''GAS''and C.AGMT_STATUS_CODE=0 then 1 else 0 end)>0 then 1 else 0 end AS GAS_IND
      ,case when sum(case when C.AGMT_TYPE_CODE=''WATER''and C.AGMT_STATUS_CODE=0 then 1 else 0 end)>0 then 1 else 0 end AS WATER_IND
      ,case when sum(case when C.AGMT_TYPE_CODE=''POWER''and C.AGMT_STATUS_CODE=0 then 1 else 0 end)>0 then 1 else 0 end AS POWER_IND   
      ,case when sum(case when C.AGMT_TYPE_CODE=''TELE_COMM''and C.AGMT_STATUS_CODE=0 then 1 else 0 end)>0 then 1 else 0 end AS TELE_COMM_IND   
      ,case when sum(case when C.AGMT_TYPE_CODE=''ALIPAY''and C.AGMT_STATUS_CODE=0 then 1 else 0 end)>0 then 1 else 0 end AS ALIPAY_IND 
      ,case when sum(case when C.AGMT_TYPE_CODE=''BP''and C.AGMT_STATUS_CODE=0 then 1 else 0 end)>0 then 1 else 0 end AS BP_IND 
      ,case when sum(case when C.AGMT_TYPE_CODE=''PAYROLL''and C.AGMT_STATUS_CODE=0 then 1 else 0 end)>0 then 1 else 0 end AS PAYROLL_IND   
      ,case when sum(case when C.AGMT_TYPE_CODE=''ENTRUST''and C.AGMT_STATUS_CODE=0 then 1 else 0 end)>0 then 1 else 0 end AS ENTRUST_IND   
      ,case when sum(case when C.AGMT_TYPE_CODE=''MB''and C.AGMT_STATUS_CODE=0 then 1 else 0 end)>0 then 1 else 0 end AS MB_IND 
      ,case when sum(case when C.AGMT_TYPE_CODE=''SMS''and C.AGMT_STATUS_CODE=0 then 1 else 0 end)>0 then 1 else 0 end AS SMS_IND   
      ,case when sum(case when C.AGMT_TYPE_CODE=''TELE''and C.AGMT_STATUS_CODE=0 then 1 else 0 end)>0 then 1 else 0 end AS TELE_IND 
      ,case when sum(case when C.AGMT_TYPE_CODE=''WECHAT''and C.AGMT_STATUS_CODE=0 then 1 else 0 end)>0 then 1 else 0 end AS WECHAT_IND 
      ,case when sum(case when C.AGMT_TYPE_CODE=''NB''and C.AGMT_STATUS_CODE=0 then 1 else 0 end)>0 then 1 else 0 end AS NB_IND 
      ,case when sum(case when C.AGMT_TYPE_CODE=''NB_ENT''and C.AGMT_STATUS_CODE=0 then 1 else 0 end)>0 then 1 else 0 end AS NB_ENT_IND 
      ,case when sum(case when C.AGMT_TYPE_CODE=''NB_LIFE''and C.AGMT_STATUS_CODE=0 then 1 else 0 end)>0 then 1 else 0 end AS NB_LIFE_IND   
      ,case when sum(case when C.AGMT_TYPE_CODE=''NB_WM''and C.AGMT_STATUS_CODE=0 then 1 else 0 end)>0 then 1 else 0 end AS NB_WM_IND   
      ,case when sum(case when C.AGMT_TYPE_CODE=''IC_CARD''and C.AGMT_STATUS_CODE=0 then 1 else 0 end)>0 then 1 else 0 end AS IC_CARD_IND 
  
  FROM MMAPST.ST_CUSTOMER A
  LEFT JOIN MMAPST.ST_CUST_AGREEMENT C
  ON A.CUSTOMER_ID = C.CUSTOMER_ID
  GROUP BY A.CUSTOMER_ID
  ';
  EXECUTE IMMEDIATE DM_SQL;
  COMMIT;
  --此表为定期跑批，因此不用检查源表中数据日期--
  --备份目标表--
  --查询DM层表中是否有"当日"数据，是否为重跑。
    DM_SQL := ' SELECT COUNT(1) FROM MMAPDM.DM_CUST_STATIC_IND WHERE PERIOD_ID='||DM_TODAY;
    EXECUTE  IMMEDIATE DM_SQL  INTO ST_ROW;
    IF ST_ROW=0
    THEN
      DM_SQL :='TRUNCATE TABLE MMAPDM.DM_CUST_STATIC_IND_PRE' ;   -- 删除备份表中数据
      EXECUTE IMMEDIATE DM_SQL;
      COMMIT;
      DM_SQL :='INSERT INTO MMAPDM.DM_CUST_STATIC_IND_PRE SELECT * FROM MMAPDM.DM_CUST_STATIC_IND';   -- 备份表中数据
      EXECUTE IMMEDIATE DM_SQL;
      IO_ROW := IO_ROW+SQL%ROWCOUNT ;
      COMMIT;
    END IF;
  
  -----向目标表中插入数据-----
  BEGIN
    --清空目标表
    DM_SQL :='TRUNCATE TABLE MMAPDM.DM_CUST_STATIC_IND';
    EXECUTE IMMEDIATE DM_SQL;
    COMMIT; 
    
    DM_SQL :='
      SELECT
       TO_NUMBER(TO_CHAR((SYSDATE),''YYYYMMDD'')) AS ETL_DATE
      ,A.TX_DATE  
      ,A.PERIOD_ID    
      ,A.CUSTOMER_ID  
      ,B.ACPT_ATM_IND 
      ,B.ACPT_DM_IND  
      ,B.ACPT_EDM_IND 
      ,B.ACPT_MB_IND  
      ,B.ACPT_NB_IND  
      ,B.ACPT_PERSON_IND  
      ,B.ACPT_SMS_IND 
      ,B.ACPT_WEBATM_IND  
      ,B.ACPT_WECHAT_IND  
      ,C.GAS_IND  
      ,C.WATER_IND    
      ,C.POWER_IND    
      ,C.TELE_COMM_IND    
      ,C.ALIPAY_IND   
      ,C.BP_IND   
      ,C.PAYROLL_IND  
      ,C.ENTRUST_IND  
      ,C.MB_IND   
      ,C.SMS_IND  
      ,C.TELE_IND 
      ,C.WECHAT_IND   
      ,C.NB_IND   
      ,C.NB_ENT_IND   
      ,C.NB_LIFE_IND  
      ,C.NB_WM_IND    
      ,C.IC_CARD_IND
      ,NULL AS IND_1
      ,NULL AS IND_2
      ,NULL AS IND_3
      ,NULL AS IND_4
      ,NULL AS IND_5
      ,NULL AS IND_6
      ,NULL AS IND_7
      ,NULL AS IND_8
      ,NULL AS IND_9
      ,NULL AS IND_10
      ,NULL AS IND_11
      ,NULL AS IND_12
      ,NULL AS IND_13
      ,NULL AS IND_14
      ,NULL AS IND_15
      ,NULL AS IND_16
      ,NULL AS IND_17
      ,NULL AS IND_18
      ,NULL AS IND_19
      ,NULL AS IND_20
      
      FROM MMAPST.ST_CUSTOMER A
      LEFT JOIN MMAPDM.TMP_CUST_CONTACT_RULE B
      ON A.CUSTOMER_ID = B.CUSTOMER_ID
      LEFT JOIN MMAPDM.TMP_CUST_AGREEMENT C
      ON A.CUSTOMER_ID = C.CUSTOMER_ID
      ';
    --计数器初始化 
    V_COUNT  := 0;
    --批量插入当日数据
    OPEN C1 FOR DM_SQL;
    LOOP
      FETCH C1 INTO V_DM_CUST_STATIC_IND;
      EXIT WHEN C1%NOTFOUND;
      INSERT INTO DM_CUST_STATIC_IND VALUES V_DM_CUST_STATIC_IND;
      IO_ROW := IO_ROW+SQL%ROWCOUNT;
      V_COUNT := V_COUNT + 1;
      IF (V_COMMITNUM>0 AND (MOD(V_COUNT, V_COMMITNUM)) = 0) THEN        
        COMMIT; 
      END IF;  
    END LOOP;
    COMMIT;
    CLOSE C1;
  END;


  
  DM_SQL:= 'DROP TABLE MMAPDM.TMP_CUST_CONTACT_RULE PURGE';
  EXECUTE IMMEDIATE DM_SQL;
  DM_SQL:= 'DROP TABLE MMAPDM.TMP_CUST_AGREEMENT PURGE';
  EXECUTE IMMEDIATE DM_SQL;
  
  SELECT SYSDATE INTO V_END_TIMESTAMP FROM DUAL;  -- 加载程序运行结束时间
  
    IO_STATUS := 0 ;
    IO_SQLERR := 'SUSSCESS';
    P_MMAPDM_WRITE_LOGS(PROCEDURE_NAME,IO_STATUS,IO_ROW,V_START_TIMESTAMP,V_END_TIMESTAMP,IO_SQLERR);
    COMMIT;
    EXCEPTION
      WHEN OTHERS  THEN
    ROLLBACK;
    --恢复备份数据
    P_DM_FULL_ROLLBACK(TABLE_NAME,IO_ROW);
    IO_STATUS := 9;
    IO_SQLERR := SQLCODE ||  SQLERRM;
  
    SELECT SYSDATE INTO V_END_TIMESTAMP FROM DUAL;
    P_MMAPDM_WRITE_LOGS(PROCEDURE_NAME,IO_STATUS,IO_ROW,V_START_TIMESTAMP,V_END_TIMESTAMP,IO_SQLERR);
  
  
END P_DM_CUST_STATIC_IND;
