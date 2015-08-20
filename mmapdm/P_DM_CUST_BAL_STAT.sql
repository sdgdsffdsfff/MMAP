CREATE OR REPLACE PROCEDURE "P_DM_CUST_BAL_STAT" 


AS
    VO_SQLERR  VARCHAR2(255);
    TABLE_NAME VARCHAR2(125) :='DM_CUST_BAL_STAT'; -- table name
    PROCEDURE_NAME VARCHAR2(125) :='P_'||TABLE_NAME;
    DM_SQL VARCHAR2(20000); -- the variable to loading the SQL statment 
    IO_ROW INTEGER;
    IO_STATUS INTEGER;
    V_START_TIMESTAMP TIMESTAMP;    --the start time of procedures
    V_END_TIMESTAMP   TIMESTAMP;    --the end time of procedures
    COUNT_NUM INTEGER;

BEGIN
  
SELECT SYSDATE INTO V_START_TIMESTAMP FROM dual;    -- 加载程序运行开始时间

-----------------------源表ST_CUST_BAL转置并翻倍-----------------------
 
SELECT COUNT(1) INTO COUNT_NUM
FROM USER_TABLES 
WHERE TABLE_NAME = 'TMP_CUST_BAL_T'
;
IF COUNT_NUM >0
THEN 
  DM_SQL := 'DROP TABLE MMAPDM.TMP_CUST_BAL_T PURGE';
EXECUTE  IMMEDIATE DM_SQL;
COMMIT;
ELSE
IO_ROW := COUNT_NUM;
END IF;

DM_SQL:= 'CREATE TABLE MMAPDM.TMP_CUST_BAL_T AS
SELECT
   B.ETL_DATE
  ,B.TX_DATE
  ,B.PERIOD_ID
  ,B.CUSTOMER_ID
  ,CASE A.DAYOFYEAR
        WHEN 1 THEN ''CD''
        WHEN 2 THEN ''TD''
        WHEN 3 THEN ''LOAN''
        WHEN 4 THEN ''NDEBT''
        WHEN 5 THEN ''FOUND''
        WHEN 6 THEN ''FIN''
        WHEN 7 THEN ''INSURE''
        WHEN 8 THEN ''NOBLE''
        WHEN 9 THEN ''CREDIT''
        WHEN 10 THEN ''ALL''
   END AS PROD_TYPE
  ,CASE A.DAYOFYEAR
        WHEN 1 THEN B.CUST_CD_BAL_LC
        WHEN 2 THEN B.CUST_TD_BAL_LC
        WHEN 3 THEN B.CUST_LOAN_BAL_LC
        WHEN 4 THEN B.CUST_NDEBT_BAL_LC
        WHEN 5 THEN B.CUST_FOUND_BAL_LC
        WHEN 6 THEN B.CUST_FIN_BAL_LC
        WHEN 7 THEN B.CUST_INSURE_BAL_LC
        WHEN 8 THEN B.CUST_NOBLE_BAL_LC
        WHEN 9 THEN B.CUST_CREDIT_BAL_LC
        WHEN 10 THEN B.CUST_ALL_BAL_LC
   END AS CUST_BAL_LC
  ,CASE A.DAYOFYEAR
        WHEN 1 THEN B.CUST_CD_BAL_FC
        WHEN 2 THEN B.CUST_TD_BAL_FC
        WHEN 3 THEN B.CUST_LOAN_BAL_FC
        WHEN 9 THEN B.CUST_CREDIT_BAL_FC
        WHEN 10 THEN B.CUST_ALL_BAL_FC
   END AS CUST_BAL_FC
  ,CASE A.DAYOFYEAR
        WHEN 1 THEN B.CUST_CD_BAL_LC+B.CUST_CD_BAL_FC
        WHEN 2 THEN B.CUST_TD_BAL_LC+B.CUST_TD_BAL_FC
        WHEN 3 THEN B.CUST_LOAN_BAL_LC+B.CUST_LOAN_BAL_FC
        WHEN 4 THEN B.CUST_NDEBT_BAL_LC
        WHEN 5 THEN B.CUST_FOUND_BAL_LC
        WHEN 6 THEN B.CUST_FIN_BAL_LC
        WHEN 7 THEN B.CUST_INSURE_BAL_LC
        WHEN 8 THEN B.CUST_NOBLE_BAL_LC
        WHEN 9 THEN B.CUST_CREDIT_BAL_LC+B.CUST_CREDIT_BAL_FC
        WHEN 10 THEN B.CUST_ALL_BAL
   END AS CUST_BAL
FROM 
(
SELECT A.DAYOFYEAR
FROM MMAPST.MID_CALENDAR A
WHERE A.DAYOFYEAR<=10
GROUP BY A.DAYOFYEAR
)A
LEFT JOIN MMAPST.ST_CUST_BAL B
ON 1=1
ORDER BY A.DAYOFYEAR'
;
EXECUTE  IMMEDIATE DM_SQL;
COMMIT;

-----------------------客户活期产品余额周频率统计-----------------------
/*
    0. 更新数据标签
*/
UPDATE MMAPDM.DM_CUST_BAL_STAT SET NEW_FLAG = '0';
/*
    1. 建立临时表
*/

-----取前一天数据-----
SELECT COUNT(1) INTO COUNT_NUM
FROM USER_TABLES 
WHERE TABLE_NAME = 'TMP_CUST_BAL_CAL'
;
IF COUNT_NUM >0
THEN 
  DM_SQL := 'DROP TABLE MMAPDM.TMP_CUST_BAL_CAL';
EXECUTE  IMMEDIATE DM_SQL;
COMMIT;
ELSE
IO_ROW := COUNT_NUM;
END IF; 

DM_SQL:= 'CREATE TABLE MMAPDM.TMP_CUST_BAL_CAL AS
SELECT
   a.ETL_DATE     
  ,a.TX_DATE
  ,''W''                        AS FREQ                 --频度（D\W\M\Q\Y）需要修改
  ,b.YEAR
  ,''0''                        AS FREQ_DIFF
  ,a.PROD_TYPE                  AS PROD_TYPE          --产品大类-活期 需要修改   
  ,a.CUSTOMER_ID                                        --客户号
  ,a.PERIOD_ID        
  ,b.DAYOFWEEK                                          --需要修改
  ,b.WEEKOFYEAR                                         --需要修改
  ,a.CUST_BAL_LC                AS CUST_BAL_LC             --活期（CD）产品的（人民币LC）当期余额
  ,c.CUST_BAL_LC                AS CUST_BAL_CWS_LC         --同期余额
  ,d.CUST_BAL_LC                AS CUST_BAL_SQT_LC         --上期余额
  ,(CASE WHEN b.DAYOFWEEK = ''1'' THEN a.CUST_BAL_LC
    ELSE 
        greatest(NVL(a.CUST_BAL_LC,0),NVL(e.CUST_BAL_MAX_LC,0)) END)
                        AS CUST_BAL_MAX_LC       --余额_最大值
  ,(CASE WHEN b.DAYOFWEEK = ''1'' THEN a.PERIOD_ID
    ELSE
        (CASE WHEN NVL(a.CUST_BAL_LC,0) - NVL(e.CUST_BAL_MAX_LC,0) > 0 THEN a.PERIOD_ID ELSE e.CUST_BAL_MAX_DATE_LC END) END) 
                        AS CUST_BAL_MAX_DATE_LC    --余额_最大值_日期
  ,(CASE WHEN b.DAYOFWEEK = ''1'' THEN a.CUST_BAL_LC
    ELSE 
        least(NVL(a.CUST_BAL_LC,0),NVL(e.CUST_BAL_MIN_LC,0)) END)
                        AS CUST_BAL_MIN_LC         --余额_最小值
  ,(CASE WHEN b.DAYOFWEEK = ''1'' THEN a.PERIOD_ID
    ELSE
        (CASE WHEN NVL(a.CUST_BAL_LC,0) - NVL(e.CUST_BAL_MIN_LC,0) < 0 THEN a.PERIOD_ID ELSE e.CUST_BAL_MIN_DATE_LC END) END)
                        AS CUST_BAL_MIN_DATE_LC    --余额_最小值_日期
  ,(CASE WHEN b.DAYOFWEEK = ''1''THEN a.CUST_BAL_LC
    ELSE 
        ((NVL(e.CUST_BAL_AVG_LC,0) * (b.DAYOFWEEK-1) + NVL(a.CUST_BAL_LC,0)) / b.DAYOFWEEK) END)
                            AS CUST_BAL_AVG_LC         --日平均余额
  ,c.CUST_BAL_AVG_LC        AS CUST_BAL_AVG_CWS_LC     --日平均余额_同期
  ,d.CUST_BAL_AVG_LC        AS CUST_BAL_AVG_SQT_LC     --日平均余额_上期
  ,a.CUST_BAL_FC            AS CUST_BAL_FC          --外币余额
  ,c.CUST_BAL_FC           AS CUST_BAL_CWS_FC      --外币余额_同期
  ,d.CUST_BAL_FC           AS CUST_BAL_SQT_FC      --外币余额_上期
  ,(CASE WHEN b.DAYOFWEEK = ''1'' THEN a.CUST_BAL_FC
    ELSE 
        greatest(NVL(a.CUST_BAL_FC,0),NVL(e.CUST_BAL_MAX_FC,0)) END)
                        AS CUST_BAL_MAX_FC      --外币余额_最大值
  ,(CASE WHEN b.DAYOFWEEK = ''1'' THEN a.PERIOD_ID
    ELSE
        (CASE WHEN NVL(a.CUST_BAL_FC,0) - NVL(e.CUST_BAL_MAX_FC,0) > 0 THEN a.PERIOD_ID ELSE e.CUST_BAL_MAX_DATE_FC END) END)
                        AS CUST_BAL_MAX_DATE_FC --外币余额_最大值_日期
  ,(CASE WHEN b.DAYOFWEEK = ''1'' THEN a.CUST_BAL_FC
    ELSE
        least(NVL(a.CUST_BAL_FC,0),NVL(e.CUST_BAL_MIN_FC,0)) END)
                        AS CUST_BAL_MIN_FC      --外币余额_最小值
  ,(CASE WHEN b.DAYOFWEEK = ''1'' THEN a.PERIOD_ID
    ELSE
        (CASE WHEN NVL(a.CUST_BAL_FC,0) - NVL(e.CUST_BAL_MIN_FC,0) < 0 THEN a.PERIOD_ID ELSE e.CUST_BAL_MIN_DATE_FC END) END)
                        AS CUST_BAL_MIN_DATE_FC --外币余额_最小值_日期
  ,(CASE WHEN b.DAYOFWEEK = ''1'' THEN a.CUST_BAL_FC
    ELSE
        ((NVL(e.CUST_BAL_AVG_FC,0) * (b.DAYOFWEEK-1) + NVL(a.CUST_BAL_FC,0)) / b.DAYOFWEEK) END)
                        AS CUST_BAL_AVG_FC      --外币日平均余额
  ,c.CUST_BAL_AVG_FC    AS CUST_BAL_AVG_CWS_FC  --外币日平均余额_同期
  ,d.CUST_BAL_AVG_FC    AS  CUST_BAL_AVG_SQT_FC --外币日平均余额_同期
  ,a.CUST_BAL                AS CUST_BAL             --合计当期余额
  ,c.CUST_BAL                AS CUST_BAL_CWS         --合计同期余额
  ,d.CUST_BAL                AS CUST_BAL_SQT         --合计上期余额
  ,(CASE WHEN b.DAYOFWEEK = ''1'' THEN a.CUST_BAL
    ELSE 
        greatest(NVL(a.CUST_BAL,0),NVL(e.CUST_BAL_MAX,0)) END)
                        AS CUST_BAL_MAX       --合计余额_最大值
  ,(CASE WHEN b.DAYOFWEEK = ''1'' THEN a.PERIOD_ID
    ELSE
        (CASE WHEN NVL(a.CUST_BAL,0) - NVL(e.CUST_BAL_MAX,0) > 0 THEN a.PERIOD_ID ELSE e.CUST_BAL_MAX_DATE END) END) 
                        AS CUST_BAL_MAX_DATE    --合计余额_最大值_日期
  ,(CASE WHEN b.DAYOFWEEK = ''1'' THEN a.CUST_BAL
    ELSE 
        least(NVL(a.CUST_BAL,0),NVL(e.CUST_BAL_MIN,0)) END)
                        AS CUST_BAL_MIN         --合计余额_最小值
  ,(CASE WHEN b.DAYOFWEEK = ''1'' THEN a.PERIOD_ID
    ELSE
        (CASE WHEN NVL(a.CUST_BAL,0) - NVL(e.CUST_BAL_MIN,0) < 0 THEN a.PERIOD_ID ELSE e.CUST_BAL_MIN_DATE END) END)
                        AS CUST_BAL_MIN_DATE    --合计余额_最小值_日期
  ,(CASE WHEN b.DAYOFWEEK = ''1''THEN a.CUST_BAL
    ELSE 
        ((NVL(e.CUST_BAL_AVG,0) * (b.DAYOFWEEK-1) + NVL(a.CUST_BAL,0)) / b.DAYOFWEEK) END)
                        AS CUST_BAL_AVG         --合计日平均余额
  ,c.CUST_BAL_AVG       AS CUST_BAL_AVG_CWS     --合计日平均余额_同期
  ,d.CUST_BAL_AVG       AS CUST_BAL_AVG_SQT     --合计日平均余额_上期

FROM MMAPDM.TMP_CUST_BAL_T a
LEFT JOIN  MMAPST.MID_CALENDAR b
ON a.PERIOD_ID = b.PERIOD_ID
--同期c
LEFT JOIN  MMAPDM.DM_CUST_BAL_STAT c
ON b.YEAR - 1 = c.YEAR
AND b.weekofyear = c.freq_value
AND a.CUSTOMER_ID = c.CUSTOMER_ID
AND c.PROD_TYPE = a.PROD_TYPE
AND c.FREQ = ''W''
--上期d
LEFT JOIN MMAPDM.DM_CUST_BAL_STAT d
ON b.YEAR = d.YEAR
AND b.weekofyear - 1= d.freq_value
AND a.CUSTOMER_ID = d.CUSTOMER_ID
AND d.PROD_TYPE = a.PROD_TYPE
AND d.FREQ = ''W''
--最值e
LEFT JOIN MMAPDM.DM_CUST_BAL_STAT e
ON 	b.YEAR = d.YEAR
AND TO_NUMBER(TO_CHAR(a.TX_DATE - 1,''yyyymmdd'')) = e.PERIOD_ID
AND b.weekofyear = e.freq_value
AND a.CUSTOMER_ID = e.CUSTOMER_ID
AND e.PROD_TYPE = a.PROD_TYPE
AND e.FREQ = ''W''
'

;
EXECUTE  IMMEDIATE DM_SQL;
COMMIT;

/*
    2. 删除前一天历史数据
*/
DELETE FROM MMAPDM.DM_CUST_BAL_STAT
WHERE       FREQ_VALUE IN (
                SELECT DISTINCT WEEKOFYEAR 
                FROM   MMAPST.MID_CALENDAR     
                WHERE  PERIOD_ID IN (SELECT DISTINCT PERIOD_ID
                                    FROM MMAPST.ST_CUST_BAL)                
            )
AND         PERIOD_ID IN (
                SELECT DISTINCT PERIOD_ID - 1
                FROM MMAPST.ST_CUST_BAL
            )
AND         FREQ = 'W';

/*
    3. 插入当天数据
*/

DM_SQL:= 'INSERT INTO MMAPDM.DM_CUST_BAL_STAT
(
     ETL_DATE               --跑批日期(YYYYMMDD)                        
    ,TX_DATE                --数据日期(YYYYMMDD)
    ,PERIOD_ID              --日期(YYYYMMDD)
    ,CUSTOMER_ID            --客户号
    ,FREQ                   --频度（D\W\M\Q\Y）  
    ,YEAR                   --年份(YYYY)  
    ,FREQ_VALUE             --频度值(1\2\3\4)    
    ,FREQ_DIFF              --频度差(与更新日期的季度差值)   
    ,PROD_TYPE              --产品大类（活期、定期、基金、资产总额（人民币+外币）等）   
    ,CUST_BAL_LC            --余额  
    ,CUST_BAL_CWS_LC        --余额_同期      
    ,CUST_BAL_SQT_LC        --余额_上期        
    ,CUST_BAL_MAX_LC        --余额_最大值          
    ,CUST_BAL_MAX_DATE_LC   --余额_最大值_日期                 
    ,CUST_BAL_MIN_LC        --余额_最小值                  
    ,CUST_BAL_MIN_DATE_LC   --余额_最小值_日期                       
    ,CUST_BAL_AVG_LC        --日平均余额              
    ,CUST_BAL_AVG_CWS_LC    --日平均余额_同期                      
    ,CUST_BAL_AVG_SQT_LC    --日平均余额_上期                        
    ,CUST_BAL_FC            --外币余额                
    ,CUST_BAL_CWS_FC        --外币余额_同期
    ,CUST_BAL_SQT_FC        --外币余额_上期
    ,CUST_BAL_MAX_FC        --外币余额_最大值
    ,CUST_BAL_MAX_DATE_FC   --外币余额_最大值_日期
    ,CUST_BAL_MIN_FC        --外币余额_最小值
    ,CUST_BAL_MIN_DATE_FC   --外币余额_最小值_日期
    ,CUST_BAL_AVG_FC        --外币日平均余额
    ,CUST_BAL_AVG_CWS_FC    --外币日平均余额_同期
    ,CUST_BAL_AVG_SQT_FC    --外币日平均余额_上期
    ,CUST_BAL               --合计余额  
    ,CUST_BAL_CWS           --合计余额_同期      
    ,CUST_BAL_SQT           --合计余额_上期        
    ,CUST_BAL_MAX           --合计余额_最大值          
    ,CUST_BAL_MAX_DATE      --合计余额_最大值_日期                 
    ,CUST_BAL_MIN           --合计余额_最小值                  
    ,CUST_BAL_MIN_DATE      --合计余额_最小值_日期                       
    ,CUST_BAL_AVG           --合计日平均余额              
    ,CUST_BAL_AVG_CWS       --合计日平均余额_同期                      
    ,CUST_BAL_AVG_SQT       --合计日平均余额_上期 
    ,NEW_FLAG    
)
SELECT 
     ETL_DATE     
    ,TX_DATE      
    ,PERIOD_ID    
    ,CUSTOMER_ID
    ,''W'' AS FREQ        --频度（D\W\M\Q\Y）
    ,YEAR
    ,WEEKOFYEAR
    ,0 AS FREQ_DIFF
    ,PROD_TYPE  --产品大类（活期、定期、基金、资产总额（人民币
    ,CUST_BAL_LC         
    ,CUST_BAL_CWS_LC     
    ,CUST_BAL_SQT_LC     
    ,CUST_BAL_MAX_LC     
    ,CUST_BAL_MAX_DATE_LC
    ,CUST_BAL_MIN_LC     
    ,CUST_BAL_MIN_DATE_LC
    ,CUST_BAL_AVG_LC     
    ,CUST_BAL_AVG_CWS_LC 
    ,CUST_BAL_AVG_SQT_LC 
    ,CUST_BAL_FC
    ,CUST_BAL_CWS_FC    
    ,CUST_BAL_SQT_FC
    ,CUST_BAL_MAX_FC
    ,CUST_BAL_MAX_DATE_FC
    ,CUST_BAL_MIN_FC
    ,CUST_BAL_MIN_DATE_FC
    ,CUST_BAL_AVG_FC
    ,CUST_BAL_AVG_CWS_FC
    ,CUST_BAL_AVG_SQT_FC
    ,CUST_BAL
    ,CUST_BAL_CWS   
    ,CUST_BAL_SQT
    ,CUST_BAL_MAX
    ,CUST_BAL_MAX_DATE
    ,CUST_BAL_MIN
    ,CUST_BAL_MIN_DATE
    ,CUST_BAL_AVG
    ,CUST_BAL_AVG_CWS
    ,CUST_BAL_AVG_SQT   
    ,1 AS NEW_FLAG
FROM  MMAPDM.TMP_CUST_BAL_CAL'
;
EXECUTE  IMMEDIATE DM_SQL;
IO_ROW := SQL%ROWCOUNT ;
COMMIT;

/*
    4. 更新频度差
*/

UPDATE  MMAPDM.DM_CUST_BAL_STAT
SET     FREQ_DIFF = FREQ_DIFF + 1 
WHERE   FREQ = 'W'
AND     NEW_FLAG = 0
AND     (SELECT DISTINCT a.DAYOFWEEK FROM MMAPST.ST_CUST_BAL b
          LEFT JOIN MMAPST.MID_CALENDAR a
          ON a.PERIOD_ID=b.PERIOD_ID) = 1
;

/*
    5. 删除过期数据（周数据留存12期）
*/

DELETE FROM MMAPDM.DM_CUST_BAL_STAT
WHERE       FREQ_DIFF >= 12
AND         FREQ = 'W';

-----------------------客户活期产品余额月频率统计-----------------------

/*
    1. 更新临时表
*/
SELECT COUNT(1) INTO COUNT_NUM
FROM USER_TABLES 
WHERE TABLE_NAME = 'TMP_CUST_BAL_CAL'
;
IF COUNT_NUM >0
THEN 
  DM_SQL := 'DROP TABLE MMAPDM.TMP_CUST_BAL_CAL PURGE';
EXECUTE  IMMEDIATE DM_SQL;
COMMIT;
ELSE
IO_ROW := COUNT_NUM;
END IF;

DM_SQL:= 'CREATE TABLE MMAPDM.TMP_CUST_BAL_CAL AS
SELECT
   a.ETL_DATE     
  ,a.TX_DATE
  ,''M''                       AS FREQ                    --频度（D\W\M\Q\Y）需要修改
  ,b.YEAR
  ,''0''                AS FREQ_DIFF
  ,a.PROD_TYPE               AS PROD_TYPE           --产品大类-活期 需要修改   
  ,a.CUSTOMER_ID                                --客户号
  ,a.PERIOD_ID        
  ,b.DAYOFMONTH                                 --需要修改
  ,b.MONTH                              --需要修改
  ,a.CUST_BAL_LC            AS CUST_BAL_LC             --活期（CD）产品的（人民币LC）当期余额
  ,c.CUST_BAL_LC               AS CUST_BAL_CWS_LC         --同期余额
  ,d.CUST_BAL_LC               AS CUST_BAL_SQT_LC         --上期余额
  ,(CASE WHEN b.DAYOFMONTH = ''1'' THEN a.CUST_BAL_LC
    ELSE 
        greatest(NVL(a.CUST_BAL_LC,0),NVL(e.CUST_BAL_MAX_LC,0)) END)
                        AS CUST_BAL_MAX_LC       --余额_最大值
  ,(CASE WHEN b.DAYOFMONTH = ''1'' THEN a.PERIOD_ID
    ELSE
        (CASE WHEN NVL(a.CUST_BAL_LC,0) - NVL(e.CUST_BAL_MAX_LC,0) > 0 THEN a.PERIOD_ID ELSE e.CUST_BAL_MAX_DATE_LC END) END) 
                        AS CUST_BAL_MAX_DATE_LC    --余额_最大值_日期
  ,(CASE WHEN b.DAYOFMONTH = ''1'' THEN a.CUST_BAL_LC
    ELSE 
        least(NVL(a.CUST_BAL_LC,0),NVL(e.CUST_BAL_MIN_LC,0)) END)
                        AS CUST_BAL_MIN_LC         --余额_最小值
  ,(CASE WHEN b.DAYOFMONTH = ''1'' THEN a.PERIOD_ID
    ELSE
        (CASE WHEN NVL(a.CUST_BAL_LC,0) - NVL(e.CUST_BAL_MIN_LC,0) < 0 THEN a.PERIOD_ID ELSE e.CUST_BAL_MIN_DATE_LC END) END)
                        AS CUST_BAL_MIN_DATE_LC    --余额_最小值_日期
  ,(CASE WHEN b.DAYOFMONTH = ''1'' THEN a.CUST_BAL_LC
    ELSE 
        ((NVL(e.CUST_BAL_AVG_LC,0) * (b.DAYOFMONTH-1) + NVL(a.CUST_BAL_LC,0)) / b.DAYOFMONTH) END)
                        AS CUST_BAL_AVG_LC         --日平均余额
  ,c.CUST_BAL_AVG_LC       AS CUST_BAL_AVG_CWS_LC     --日平均余额_同期
  ,d.CUST_BAL_AVG_LC       AS CUST_BAL_AVG_SQT_LC     --日平均余额_上期
  ,a.CUST_BAL_FC        AS CUST_BAL_FC             --外币余额
  ,c.CUST_BAL_FC       AS CUST_BAL_CWS_FC      --外币余额_同期
  ,d.CUST_BAL_FC       AS CUST_BAL_SQT_FC      --外币余额_上期
  ,(CASE WHEN b.DAYOFMONTH = ''1'' THEN a.CUST_BAL_FC
    ELSE 
        greatest(NVL(a.CUST_BAL_FC,0),NVL(e.CUST_BAL_MAX_FC,0)) END)
                        AS CUST_BAL_MAX_FC      --外币余额_最大值
  ,(CASE WHEN b.DAYOFMONTH = ''1'' THEN a.PERIOD_ID
    ELSE
        (CASE WHEN NVL(a.CUST_BAL_FC,0) - NVL(e.CUST_BAL_MAX_FC,0) > 0 THEN a.PERIOD_ID ELSE e.CUST_BAL_MAX_DATE_FC END) END)
                        AS CUST_BAL_MAX_DATE_FC --外币余额_最大值_日期
  ,(CASE WHEN b.DAYOFMONTH = ''1'' THEN a.CUST_BAL_FC
    ELSE
        least(NVL(a.CUST_BAL_FC,0),NVL(e.CUST_BAL_MIN_FC,0)) END)
                        AS CUST_BAL_MIN_FC      --外币余额_最小值
  ,(CASE WHEN b.DAYOFMONTH = ''1'' THEN a.PERIOD_ID
    ELSE
        (CASE WHEN NVL(a.CUST_BAL_FC,0) - NVL(e.CUST_BAL_MIN_FC,0) < 0 THEN a.PERIOD_ID ELSE e.CUST_BAL_MIN_DATE_FC END) END)
                        AS CUST_BAL_MIN_DATE_FC --外币余额_最小值_日期
  ,(CASE WHEN b.DAYOFMONTH = ''1'' THEN a.CUST_BAL_FC
    ELSE
        ((NVL(e.CUST_BAL_AVG_FC,0) * (b.DAYOFMONTH-1) + NVL(a.CUST_BAL_FC,0)) / b.DAYOFMONTH) END)
                        AS CUST_BAL_AVG_FC      --外币日平均余额
  ,c.CUST_BAL_AVG_FC    AS CUST_BAL_AVG_CWS_FC  --外币日平均余额_同期
  ,d.CUST_BAL_AVG_FC    AS  CUST_BAL_AVG_SQT_FC --外币日平均余额_同期
  ,a.CUST_BAL               AS CUST_BAL             --合计当期余额
  ,c.CUST_BAL               AS CUST_BAL_CWS         --合计同期余额
  ,d.CUST_BAL               AS CUST_BAL_SQT         --合计上期余额
  ,(CASE WHEN b.DAYOFMONTH = ''1'' THEN a.CUST_BAL
    ELSE 
        greatest(NVL(a.CUST_BAL,0),NVL(e.CUST_BAL_MAX,0)) END)
                        AS CUST_BAL_MAX       --合计余额_最大值
  ,(CASE WHEN b.DAYOFMONTH = ''1'' THEN a.PERIOD_ID
    ELSE
        (CASE WHEN NVL(a.CUST_BAL,0) - NVL(e.CUST_BAL_MAX,0) > 0 THEN a.PERIOD_ID ELSE e.CUST_BAL_MAX_DATE END) END) 
                        AS CUST_BAL_MAX_DATE    --合计余额_最大值_日期
  ,(CASE WHEN b.DAYOFMONTH = ''1'' THEN a.CUST_BAL
    ELSE 
        least(NVL(a.CUST_BAL,0),NVL(e.CUST_BAL_MIN,0)) END)
                        AS CUST_BAL_MIN         --合计余额_最小值
  ,(CASE WHEN b.DAYOFMONTH = ''1'' THEN a.PERIOD_ID
    ELSE
        (CASE WHEN NVL(a.CUST_BAL,0) - NVL(e.CUST_BAL_MIN,0) < 0 THEN a.PERIOD_ID ELSE e.CUST_BAL_MIN_DATE END) END)
                        AS CUST_BAL_MIN_DATE    --合计余额_最小值_日期
  ,(CASE WHEN b.DAYOFMONTH = ''1'' THEN a.CUST_BAL
    ELSE 
        ((NVL(e.CUST_BAL_AVG,0) * (b.DAYOFMONTH-1) + NVL(a.CUST_BAL,0)) / b.DAYOFMONTH) END)
                        AS CUST_BAL_AVG         --合计日平均余额
  ,c.CUST_BAL_AVG       AS CUST_BAL_AVG_CWS     --合计日平均余额_同期
  ,d.CUST_BAL_AVG       AS CUST_BAL_AVG_SQT     --合计日平均余额_上期
 
FROM MMAPDM.TMP_CUST_BAL_T a
LEFT JOIN  MMAPST.MID_CALENDAR b
ON a.PERIOD_ID = b.PERIOD_ID
--同期c
LEFT JOIN  MMAPDM.DM_CUST_BAL_STAT c
ON b.YEAR - 1 = c.YEAR
AND b.MONTH = c.freq_value
AND a.CUSTOMER_ID = c.CUSTOMER_ID
AND c.PROD_TYPE = a.PROD_TYPE
AND c.FREQ = ''M''
--上期d
LEFT JOIN MMAPDM.DM_CUST_BAL_STAT d
ON b.YEAR = d.YEAR
AND b.MONTH - 1= d.freq_value
AND a.CUSTOMER_ID = d.CUSTOMER_ID
AND d.PROD_TYPE = a.PROD_TYPE
AND d.FREQ = ''M''
--最值e
LEFT JOIN MMAPDM.DM_CUST_BAL_STAT e
ON TO_NUMBER(TO_CHAR(a.TX_DATE - 1,''yyyymmdd'')) = e.PERIOD_ID
AND b.MONTH = e.freq_value
AND a.CUSTOMER_ID = e.CUSTOMER_ID
AND e.PROD_TYPE = a.PROD_TYPE
AND d.FREQ = ''M''
'
;
EXECUTE  IMMEDIATE DM_SQL;
COMMIT;

/*
    2. 删除前一天历史数据
*/
DELETE FROM MMAPDM.DM_CUST_BAL_STAT
WHERE       FREQ_VALUE IN (
                SELECT DISTINCT MONTH 
                FROM   MMAPST.MID_CALENDAR     
                WHERE  PERIOD_ID IN (SELECT DISTINCT PERIOD_ID
                                    FROM MMAPST.ST_CUST_BAL)                
            )
AND         PERIOD_ID IN (
                SELECT DISTINCT PERIOD_ID - 1
                FROM MMAPST.ST_CUST_BAL
            )
AND         FREQ = 'M';



/*
    3. 插入当天数据
*/

DM_SQL:= 'INSERT INTO MMAPDM.DM_CUST_BAL_STAT
(
     ETL_DATE               --跑批日期(YYYYMMDD)                        
    ,TX_DATE                --数据日期(YYYYMMDD)
    ,PERIOD_ID              --日期(YYYYMMDD)
    ,CUSTOMER_ID            --客户号
    ,FREQ                   --频度（D\W\M\Q\Y）  
    ,YEAR                   --年份(YYYY)  
    ,FREQ_VALUE             --频度值(1\2\3\4)    
    ,FREQ_DIFF              --频度差(与更新日期的季度差值)   
    ,PROD_TYPE              --产品大类（活期、定期、基金、资产总额（人民币+外币）等）   
    ,CUST_BAL_LC            --余额  
    ,CUST_BAL_CWS_LC        --余额_同期      
    ,CUST_BAL_SQT_LC        --余额_上期        
    ,CUST_BAL_MAX_LC        --余额_最大值          
    ,CUST_BAL_MAX_DATE_LC   --余额_最大值_日期                 
    ,CUST_BAL_MIN_LC        --余额_最小值                  
    ,CUST_BAL_MIN_DATE_LC   --余额_最小值_日期                       
    ,CUST_BAL_AVG_LC        --日平均余额              
    ,CUST_BAL_AVG_CWS_LC    --日平均余额_同期                      
    ,CUST_BAL_AVG_SQT_LC    --日平均余额_上期                        
    ,CUST_BAL_FC            --外币余额                
    ,CUST_BAL_CWS_FC        --外币余额_同期
    ,CUST_BAL_SQT_FC        --外币余额_上期
    ,CUST_BAL_MAX_FC        --外币余额_最大值
    ,CUST_BAL_MAX_DATE_FC   --外币余额_最大值_日期
    ,CUST_BAL_MIN_FC        --外币余额_最小值
    ,CUST_BAL_MIN_DATE_FC   --外币余额_最小值_日期
    ,CUST_BAL_AVG_FC        --外币日平均余额
    ,CUST_BAL_AVG_CWS_FC    --外币日平均余额_同期
    ,CUST_BAL_AVG_SQT_FC    --外币日平均余额_上期
    ,CUST_BAL               --合计余额  
    ,CUST_BAL_CWS           --合计余额_同期      
    ,CUST_BAL_SQT           --合计余额_上期        
    ,CUST_BAL_MAX           --合计余额_最大值          
    ,CUST_BAL_MAX_DATE      --合计余额_最大值_日期                 
    ,CUST_BAL_MIN           --合计余额_最小值                  
    ,CUST_BAL_MIN_DATE      --合计余额_最小值_日期                       
    ,CUST_BAL_AVG           --合计日平均余额              
    ,CUST_BAL_AVG_CWS       --合计日平均余额_同期                      
    ,CUST_BAL_AVG_SQT       --合计日平均余额_上期 
    ,NEW_FLAG    
)
SELECT 
     ETL_DATE     
    ,TX_DATE      
    ,PERIOD_ID    
    ,CUSTOMER_ID
    ,''M'' AS FREQ        --频度（D\W\M\Q\Y）
    ,YEAR
    ,MONTH
    ,0 AS FREQ_DIFF
    ,PROD_TYPE  --产品大类（活期、定期、基金、资产总额（人民币
    ,CUST_BAL_LC         
    ,CUST_BAL_CWS_LC     
    ,CUST_BAL_SQT_LC     
    ,CUST_BAL_MAX_LC     
    ,CUST_BAL_MAX_DATE_LC
    ,CUST_BAL_MIN_LC     
    ,CUST_BAL_MIN_DATE_LC
    ,CUST_BAL_AVG_LC     
    ,CUST_BAL_AVG_CWS_LC 
    ,CUST_BAL_AVG_SQT_LC 
    ,CUST_BAL_FC
    ,CUST_BAL_CWS_FC    
    ,CUST_BAL_SQT_FC
    ,CUST_BAL_MAX_FC
    ,CUST_BAL_MAX_DATE_FC
    ,CUST_BAL_MIN_FC
    ,CUST_BAL_MIN_DATE_FC
    ,CUST_BAL_AVG_FC
    ,CUST_BAL_AVG_CWS_FC
    ,CUST_BAL_AVG_SQT_FC
    ,CUST_BAL
    ,CUST_BAL_CWS   
    ,CUST_BAL_SQT
    ,CUST_BAL_MAX
    ,CUST_BAL_MAX_DATE
    ,CUST_BAL_MIN
    ,CUST_BAL_MIN_DATE
    ,CUST_BAL_AVG
    ,CUST_BAL_AVG_CWS
    ,CUST_BAL_AVG_SQT   
    ,1 AS NEW_FLAG
FROM  MMAPDM.TMP_CUST_BAL_CAL'
;
EXECUTE  IMMEDIATE DM_SQL;
IO_ROW := IO_ROW+SQL%ROWCOUNT ;
COMMIT;
/*
    4. 更新频度差
*/

UPDATE  MMAPDM.DM_CUST_BAL_STAT 
SET     FREQ_DIFF = FREQ_DIFF + 1 
WHERE   FREQ = 'M' 
AND     NEW_FLAG = '0'
AND     (SELECT DISTINCT a.DAYOFMONTH FROM MMAPST.ST_CUST_BAL b
          LEFT JOIN MMAPST.MID_CALENDAR a
          ON a.PERIOD_ID=b.PERIOD_ID) = 1
;

/*
    5. 删除过期数据（月数据留存12期）
*/

DELETE FROM MMAPDM.DM_CUST_BAL_STAT
WHERE       FREQ_DIFF >= 24
AND         FREQ = 'M'
;

-----------------------客户活期产品余额季频率统计-----------------------

/*
    1. 更新临时表
*/
SELECT COUNT(1) INTO COUNT_NUM
FROM USER_TABLES 
WHERE TABLE_NAME = 'TMP_CUST_BAL_CAL'
;
IF COUNT_NUM >0
THEN 
  DM_SQL := 'DROP TABLE MMAPDM.TMP_CUST_BAL_CAL';
EXECUTE  IMMEDIATE DM_SQL;
COMMIT;
ELSE
IO_ROW := COUNT_NUM;
END IF; 

DM_SQL:= 'CREATE TABLE MMAPDM.TMP_CUST_BAL_CAL AS
SELECT
   a.ETL_DATE     
  ,a.TX_DATE
  ,''Q''                        AS FREQ                   --频度（D\W\M\Q\Y）需要修改
  ,b.YEAR
  ,''0''                        AS FREQ_DIFF
  ,a.PROD_TYPE                AS PROD_TYPE          --产品大类-活期 需要修改   
  ,a.CUSTOMER_ID                                --客户号
  ,a.PERIOD_ID        
  ,b.DAYOFQUARTER                               --需要修改
  ,b.QUARTER                                   --需要修改
  ,a.CUST_BAL_LC               AS CUST_BAL_LC             --活期（CD）产品的（人民币LC）当期余额
  ,c.CUST_BAL_LC               AS CUST_BAL_CWS_LC         --同期余额
  ,d.CUST_BAL_LC               AS CUST_BAL_SQT_LC         --上期余额
  ,(CASE WHEN b.DAYOFQUARTER = ''1'' THEN a.CUST_BAL_LC
    ELSE 
        greatest(NVL(a.CUST_BAL_LC,0),NVL(e.CUST_BAL_MAX_LC,0)) END)
                        AS CUST_BAL_MAX_LC       --余额_最大值
  ,(CASE WHEN b.DAYOFQUARTER = ''1'' THEN a.PERIOD_ID
    ELSE
        (CASE WHEN NVL(a.CUST_BAL_LC,0) - NVL(e.CUST_BAL_MAX_LC,0) > 0 THEN a.PERIOD_ID ELSE e.CUST_BAL_MAX_DATE_LC END) END) 
                        AS CUST_BAL_MAX_DATE_LC    --余额_最大值_日期
  ,(CASE WHEN b.DAYOFQUARTER = ''1'' THEN a.CUST_BAL_LC
    ELSE 
        least(NVL(a.CUST_BAL_LC,0),NVL(e.CUST_BAL_MIN_LC,0)) END)
                        AS CUST_BAL_MIN_LC         --余额_最小值
  ,(CASE WHEN b.DAYOFQUARTER = ''1'' THEN a.PERIOD_ID
    ELSE
        (CASE WHEN NVL(a.CUST_BAL_LC,0) - NVL(e.CUST_BAL_MIN_LC,0) < 0 THEN a.PERIOD_ID ELSE e.CUST_BAL_MIN_DATE_LC END) END)
                        AS CUST_BAL_MIN_DATE_LC    --余额_最小值_日期
  ,(CASE WHEN b.DAYOFQUARTER = ''1'' THEN a.CUST_BAL_LC
    ELSE 
        ((NVL(e.CUST_BAL_AVG_LC,0) * (b.DAYOFQUARTER-1) + NVL(a.CUST_BAL_LC,0)) / b.DAYOFQUARTER) END)
                        AS CUST_BAL_AVG_LC         --日平均余额
  ,c.CUST_BAL_AVG_LC       AS CUST_BAL_AVG_CWS_LC     --日平均余额_同期
  ,d.CUST_BAL_AVG_LC       AS CUST_BAL_AVG_SQT_LC     --日平均余额_上期
  ,a.CUST_BAL_FC    AS CUST_BAL_FC          --外币余额
  ,c.CUST_BAL_FC       AS CUST_BAL_CWS_FC      --外币余额_同期
  ,d.CUST_BAL_FC       AS CUST_BAL_SQT_FC      --外币余额_上期
  ,(CASE WHEN b.DAYOFQUARTER = ''1'' THEN a.CUST_BAL_FC
    ELSE 
        greatest(NVL(a.CUST_BAL_FC,0),NVL(e.CUST_BAL_MAX_FC,0)) END)
                        AS CUST_BAL_MAX_FC      --外币余额_最大值
  ,(CASE WHEN b.DAYOFQUARTER = ''1'' THEN a.PERIOD_ID
    ELSE
        (CASE WHEN NVL(a.CUST_BAL_FC,0) - NVL(e.CUST_BAL_MAX_FC,0) > 0 THEN a.PERIOD_ID ELSE e.CUST_BAL_MAX_DATE_FC END) END)
                        AS CUST_BAL_MAX_DATE_FC --外币余额_最大值_日期
  ,(CASE WHEN b.DAYOFQUARTER = ''1'' THEN a.CUST_BAL_FC
    ELSE
        least(NVL(a.CUST_BAL_FC,0),NVL(e.CUST_BAL_MIN_FC,0)) END)
                        AS CUST_BAL_MIN_FC      --外币余额_最小值
  ,(CASE WHEN b.DAYOFQUARTER = ''1'' THEN a.PERIOD_ID
    ELSE
        (CASE WHEN NVL(a.CUST_BAL_FC,0) - NVL(e.CUST_BAL_MIN_FC,0) < 0 THEN a.PERIOD_ID ELSE e.CUST_BAL_MIN_DATE_FC END) END)
                        AS CUST_BAL_MIN_DATE_FC --外币余额_最小值_日期
  ,(CASE WHEN b.DAYOFQUARTER = ''1'' THEN a.CUST_BAL_FC
    ELSE
        ((NVL(e.CUST_BAL_AVG_FC,0) * (b.DAYOFQUARTER-1) + NVL(a.CUST_BAL_FC,0)) / b.DAYOFQUARTER) END)
                        AS CUST_BAL_AVG_FC      --外币日平均余额
  ,c.CUST_BAL_AVG_FC    AS CUST_BAL_AVG_CWS_FC  --外币日平均余额_同期
  ,d.CUST_BAL_AVG_FC    AS  CUST_BAL_AVG_SQT_FC --外币日平均余额_同期
  ,a.CUST_BAL               AS CUST_BAL            --合计当期余额
  ,c.CUST_BAL               AS CUST_BAL_CWS        --合计同期余额
  ,d.CUST_BAL               AS CUST_BAL_SQT        --合计上期余额
  ,(CASE WHEN b.DAYOFQUARTER = ''1'' THEN a.CUST_BAL
    ELSE 
        greatest(NVL(a.CUST_BAL,0),NVL(e.CUST_BAL_MAX,0)) END)
                        AS CUST_BAL_MAX       --合计余额_最大值
  ,(CASE WHEN b.DAYOFQUARTER = ''1'' THEN a.PERIOD_ID
    ELSE
        (CASE WHEN NVL(a.CUST_BAL,0) - NVL(e.CUST_BAL_MAX,0) > 0 THEN a.PERIOD_ID ELSE e.CUST_BAL_MAX_DATE END) END) 
                        AS CUST_BAL_MAX_DATE    --合计余额_最大值_日期
  ,(CASE WHEN b.DAYOFQUARTER = ''1'' THEN a.CUST_BAL
    ELSE 
        least(NVL(a.CUST_BAL,0),NVL(e.CUST_BAL_MIN,0)) END)
                        AS CUST_BAL_MIN         --合计余额_最小值
  ,(CASE WHEN b.DAYOFQUARTER = ''1'' THEN a.PERIOD_ID
    ELSE
        (CASE WHEN NVL(a.CUST_BAL,0) - NVL(e.CUST_BAL_MIN,0) < 0 THEN a.PERIOD_ID ELSE e.CUST_BAL_MIN_DATE END) END)
                        AS CUST_BAL_MIN_DATE    --合计余额_最小值_日期
  ,(CASE WHEN b.DAYOFQUARTER = ''1'' THEN a.CUST_BAL
    ELSE 
        ((NVL(e.CUST_BAL_AVG,0) * (b.DAYOFQUARTER-1) + NVL(a.CUST_BAL,0)) / b.DAYOFQUARTER) END)
                        AS CUST_BAL_AVG         --合计日平均余额
  ,c.CUST_BAL_AVG       AS CUST_BAL_AVG_CWS     --合计日平均余额_同期
  ,d.CUST_BAL_AVG       AS CUST_BAL_AVG_SQT    --合计日平均余额_上期


FROM MMAPDM.TMP_CUST_BAL_T a
LEFT JOIN  MMAPST.MID_CALENDAR b
ON a.PERIOD_ID = b.PERIOD_ID
--同期c
LEFT JOIN  MMAPDM.DM_CUST_BAL_STAT c
ON b.YEAR - 1 = c.YEAR
AND b.QUARTER = c.freq_value
AND a.CUSTOMER_ID = c.CUSTOMER_ID
AND c.PROD_TYPE = a.PROD_TYPE
AND c.FREQ = ''Q''
--上期d
LEFT JOIN MMAPDM.DM_CUST_BAL_STAT d
ON b.YEAR = d.YEAR
AND b.QUARTER - 1= d.freq_value
AND a.CUSTOMER_ID = d.CUSTOMER_ID
AND d.PROD_TYPE = a.PROD_TYPE
AND d.FREQ = ''Q''
--最值e
LEFT JOIN MMAPDM.DM_CUST_BAL_STAT e
ON TO_NUMBER(TO_CHAR(a.TX_DATE - 1,''yyyymmdd'')) = e.PERIOD_ID
AND b.QUARTER = e.freq_value
AND a.CUSTOMER_ID = e.CUSTOMER_ID
AND e.PROD_TYPE = a.PROD_TYPE
AND e.FREQ = ''Q''
'
;
EXECUTE  IMMEDIATE DM_SQL;
COMMIT;

/*
    2. 删除前一天历史数据
*/
DELETE FROM MMAPDM.DM_CUST_BAL_STAT
WHERE       FREQ_VALUE IN (
                SELECT DISTINCT QUARTER 
                FROM   MMAPST.MID_CALENDAR     
                WHERE  PERIOD_ID IN (SELECT DISTINCT PERIOD_ID
                                    FROM MMAPST.ST_CUST_BAL)                
            )
AND         PERIOD_ID IN (
                SELECT DISTINCT PERIOD_ID - 1
                FROM MMAPST.ST_CUST_BAL
            )
AND         FREQ = 'Q';



/*
    3. 插入当天数据
*/

DM_SQL:= 'INSERT INTO MMAPDM.DM_CUST_BAL_STAT
(
     ETL_DATE               --跑批日期(YYYYMMDD)                        
    ,TX_DATE                --数据日期(YYYYMMDD)
    ,PERIOD_ID              --日期(YYYYMMDD)
    ,CUSTOMER_ID            --客户号
    ,FREQ                   --频度（D\W\M\Q\Y）  
    ,YEAR                   --年份(YYYY)  
    ,FREQ_VALUE             --频度值(1\2\3\4)    
    ,FREQ_DIFF              --频度差(与更新日期的季度差值)   
    ,PROD_TYPE              --产品大类（活期、定期、基金、资产总额（人民币+外币）等）   
    ,CUST_BAL_LC            --余额  
    ,CUST_BAL_CWS_LC        --余额_同期      
    ,CUST_BAL_SQT_LC        --余额_上期        
    ,CUST_BAL_MAX_LC        --余额_最大值          
    ,CUST_BAL_MAX_DATE_LC   --余额_最大值_日期                 
    ,CUST_BAL_MIN_LC        --余额_最小值                  
    ,CUST_BAL_MIN_DATE_LC   --余额_最小值_日期                       
    ,CUST_BAL_AVG_LC        --日平均余额              
    ,CUST_BAL_AVG_CWS_LC    --日平均余额_同期                      
    ,CUST_BAL_AVG_SQT_LC    --日平均余额_上期                        
    ,CUST_BAL_FC            --外币余额                
    ,CUST_BAL_CWS_FC        --外币余额_同期
    ,CUST_BAL_SQT_FC        --外币余额_上期
    ,CUST_BAL_MAX_FC        --外币余额_最大值
    ,CUST_BAL_MAX_DATE_FC   --外币余额_最大值_日期
    ,CUST_BAL_MIN_FC        --外币余额_最小值
    ,CUST_BAL_MIN_DATE_FC   --外币余额_最小值_日期
    ,CUST_BAL_AVG_FC        --外币日平均余额
    ,CUST_BAL_AVG_CWS_FC    --外币日平均余额_同期
    ,CUST_BAL_AVG_SQT_FC    --外币日平均余额_上期
    ,CUST_BAL               --合计余额  
    ,CUST_BAL_CWS           --合计余额_同期      
    ,CUST_BAL_SQT           --合计余额_上期        
    ,CUST_BAL_MAX           --合计余额_最大值          
    ,CUST_BAL_MAX_DATE      --合计余额_最大值_日期                 
    ,CUST_BAL_MIN           --合计余额_最小值                  
    ,CUST_BAL_MIN_DATE      --合计余额_最小值_日期                       
    ,CUST_BAL_AVG           --合计日平均余额              
    ,CUST_BAL_AVG_CWS       --合计日平均余额_同期                      
    ,CUST_BAL_AVG_SQT       --合计日平均余额_上期 
    ,NEW_FLAG    
)
SELECT 
     ETL_DATE     
    ,TX_DATE      
    ,PERIOD_ID    
    ,CUSTOMER_ID
    ,''Q'' AS FREQ        --频度（D\W\M\Q\Y）
    ,YEAR
    ,QUARTER
    ,0 AS FREQ_DIFF
    ,PROD_TYPE  --产品大类（活期、定期、基金、资产总额（人民币
    ,CUST_BAL_LC         
    ,CUST_BAL_CWS_LC     
    ,CUST_BAL_SQT_LC     
    ,CUST_BAL_MAX_LC     
    ,CUST_BAL_MAX_DATE_LC
    ,CUST_BAL_MIN_LC     
    ,CUST_BAL_MIN_DATE_LC
    ,CUST_BAL_AVG_LC     
    ,CUST_BAL_AVG_CWS_LC 
    ,CUST_BAL_AVG_SQT_LC 
    ,CUST_BAL_FC
    ,CUST_BAL_CWS_FC    
    ,CUST_BAL_SQT_FC
    ,CUST_BAL_MAX_FC
    ,CUST_BAL_MAX_DATE_FC
    ,CUST_BAL_MIN_FC
    ,CUST_BAL_MIN_DATE_FC
    ,CUST_BAL_AVG_FC
    ,CUST_BAL_AVG_CWS_FC
    ,CUST_BAL_AVG_SQT_FC
    ,CUST_BAL
    ,CUST_BAL_CWS   
    ,CUST_BAL_SQT
    ,CUST_BAL_MAX
    ,CUST_BAL_MAX_DATE
    ,CUST_BAL_MIN
    ,CUST_BAL_MIN_DATE
    ,CUST_BAL_AVG
    ,CUST_BAL_AVG_CWS
    ,CUST_BAL_AVG_SQT   
    ,1 AS NEW_FLAG
FROM  MMAPDM.TMP_CUST_BAL_CAL'
;
EXECUTE  IMMEDIATE DM_SQL;
IO_ROW := IO_ROW+SQL%ROWCOUNT ;
COMMIT;

/*
    4. 更新频度差
*/

UPDATE  MMAPDM.DM_CUST_BAL_STAT 
SET     FREQ_DIFF = FREQ_DIFF + 1 
WHERE   FREQ = 'Q' 
AND     NEW_FLAG = '0'
AND     (SELECT DISTINCT a.DAYOFQUARTER FROM MMAPST.ST_CUST_BAL b
          LEFT JOIN MMAPST.MID_CALENDAR a
          ON a.PERIOD_ID=b.PERIOD_ID) = 1;

/*
    5. 删除过期数据（季度数据留存8期）
*/

DELETE FROM MMAPDM.DM_CUST_BAL_STAT
WHERE       FREQ_DIFF >= 8
AND         FREQ = 'Q';

-----------------------客户活期产品余额年频率统计-----------------------

/*
    1. 更新临时表
*/
SELECT COUNT(1) INTO COUNT_NUM
FROM USER_TABLES 
WHERE TABLE_NAME = 'TMP_CUST_BAL_CAL'
;
IF COUNT_NUM >0
THEN 
  DM_SQL := 'DROP TABLE MMAPDM.TMP_CUST_BAL_CAL';
EXECUTE  IMMEDIATE DM_SQL;
COMMIT;
ELSE
IO_ROW := COUNT_NUM;
END IF; 
                                
DM_SQL:= 'CREATE TABLE MMAPDM.TMP_CUST_BAL_CAL AS
SELECT
   a.ETL_DATE     
  ,a.TX_DATE
  ,''Y''                        AS FREQ                   --频度（D\W\M\Q\Y）需要修改
  ,b.YEAR
  ,''0''                        AS FREQ_DIFF
  ,a.PROD_TYPE                    AS PROD_TYPE          --产品大类-活期 需要修改   
  ,a.CUSTOMER_ID                                --客户号
  ,a.PERIOD_ID        
  ,b.DAYOFYEAR                                  --需要修改
  ,b.YEAR                   AS  FREQ_VALUE      --需要修改
  ,a.CUST_BAL_LC               AS CUST_BAL_LC             --合计活期（CD）产品的（人民币LC）当期余额
  ,c.CUST_BAL_LC               AS CUST_BAL_CWS_LC         --合计同期余额
  ,d.CUST_BAL_LC               AS CUST_BAL_SQT_LC         --合计上期余额
  ,(CASE WHEN b.DAYOFYEAR = ''1'' THEN a.CUST_BAL_LC
    ELSE 
        greatest(NVL(a.CUST_BAL_LC,0),NVL(e.CUST_BAL_MAX_LC,0)) END)
                        AS CUST_BAL_MAX_LC       --合计余额_最大值
  ,(CASE WHEN b.DAYOFYEAR = ''1'' THEN a.PERIOD_ID
    ELSE
        (CASE WHEN NVL(a.CUST_BAL_LC,0) - NVL(e.CUST_BAL_MAX_LC,0) > 0 THEN a.PERIOD_ID ELSE e.CUST_BAL_MAX_DATE_LC END) END) 
                        AS CUST_BAL_MAX_DATE_LC    --合计余额_最大值_日期
  ,(CASE WHEN b.DAYOFYEAR = ''1'' THEN a.CUST_BAL_LC
    ELSE 
        least(NVL(a.CUST_BAL_LC,0),NVL(e.CUST_BAL_MIN_LC,0)) END)
                        AS CUST_BAL_MIN_LC         --合计余额_最小值
  ,(CASE WHEN b.DAYOFYEAR = ''1'' THEN a.PERIOD_ID
    ELSE
        (CASE WHEN NVL(a.CUST_BAL_LC,0) - NVL(e.CUST_BAL_MIN_LC,0) < 0 THEN a.PERIOD_ID ELSE e.CUST_BAL_MIN_DATE_LC END) END)
                        AS CUST_BAL_MIN_DATE_LC    --合计余额_最小值_日期
  ,(CASE WHEN b.DAYOFYEAR = ''1'' THEN a.CUST_BAL_LC
    ELSE 
        ((NVL(e.CUST_BAL_AVG_LC,0) * (b.DAYOFYEAR-1) + NVL(a.CUST_BAL_LC,0)) / b.DAYOFYEAR) END)
                        AS CUST_BAL_AVG_LC        --合计日平均余额
  ,c.CUST_BAL_AVG_LC      AS CUST_BAL_AVG_CWS_LC     --合计日平均余额_同期
  ,d.CUST_BAL_AVG_LC      AS CUST_BAL_AVG_SQT_LC     --合计日平均余额_上期
  ,a.CUST_BAL_FC          AS CUST_BAL_FC              --外币余额
  ,c.CUST_BAL_FC          AS CUST_BAL_CWS_FC      --外币余额_同期
  ,d.CUST_BAL_FC          AS CUST_BAL_SQT_FC      --外币余额_上期
  ,(CASE WHEN b.DAYOFYEAR = ''1'' THEN a.CUST_BAL_FC
    ELSE 
        greatest(NVL(a.CUST_BAL_FC,0),NVL(e.CUST_BAL_MAX_FC,0)) END)
                        AS CUST_BAL_MAX_FC      --外币余额_最大值
  ,(CASE WHEN b.DAYOFYEAR = ''1'' THEN a.PERIOD_ID
    ELSE
        (CASE WHEN NVL(a.CUST_BAL_FC,0) - NVL(e.CUST_BAL_MAX_FC,0) > 0 THEN a.PERIOD_ID ELSE e.CUST_BAL_MAX_DATE_FC END) END)
                        AS CUST_BAL_MAX_DATE_FC --外币余额_最大值_日期
  ,(CASE WHEN b.DAYOFYEAR = ''1'' THEN a.CUST_BAL_FC
    ELSE
        least(NVL(a.CUST_BAL_FC,0),NVL(e.CUST_BAL_MIN_FC,0)) END)
                        AS CUST_BAL_MIN_FC      --外币余额_最小值
  ,(CASE WHEN b.DAYOFYEAR = ''1'' THEN a.PERIOD_ID
    ELSE
        (CASE WHEN NVL(a.CUST_BAL_FC,0) - NVL(e.CUST_BAL_MIN_FC,0) < 0 THEN a.PERIOD_ID ELSE e.CUST_BAL_MIN_DATE_FC END) END)
                        AS CUST_BAL_MIN_DATE_FC --外币余额_最小值_日期
  ,(CASE WHEN b.DAYOFYEAR = ''1'' THEN a.CUST_BAL_FC
    ELSE
        ((NVL(e.CUST_BAL_AVG_FC,0) * (b.DAYOFYEAR-1) + NVL(a.CUST_BAL_FC,0)) / b.DAYOFYEAR) END)
                        AS CUST_BAL_AVG_FC      --外币日平均余额
  ,c.CUST_BAL_AVG_FC    AS CUST_BAL_AVG_CWS_FC  --外币日平均余额_同期
  ,d.CUST_BAL_AVG_FC    AS  CUST_BAL_AVG_SQT_FC --外币日平均余额_同期
  ,a.CUST_BAL               AS CUST_BAL             --合计活期（CD）产品的（人民币LC）当期余额
  ,c.CUST_BAL               AS CUST_BAL_CWS         --合计同期余额
  ,d.CUST_BAL               AS CUST_BAL_SQT         --合计上期余额
  ,(CASE WHEN b.DAYOFYEAR = ''1'' THEN a.CUST_BAL
    ELSE 
        greatest(NVL(a.CUST_BAL,0),NVL(e.CUST_BAL_MAX,0)) END)
                        AS CUST_BAL_MAX       --合计余额_最大值
  ,(CASE WHEN b.DAYOFYEAR = ''1'' THEN a.PERIOD_ID
    ELSE
        (CASE WHEN NVL(a.CUST_BAL,0) - NVL(e.CUST_BAL_MAX,0) > 0 THEN a.PERIOD_ID ELSE e.CUST_BAL_MAX_DATE END) END) 
                        AS CUST_BAL_MAX_DATE    --合计余额_最大值_日期
  ,(CASE WHEN b.DAYOFYEAR = ''1'' THEN a.CUST_BAL
    ELSE 
        least(NVL(a.CUST_BAL,0),NVL(e.CUST_BAL_MIN,0)) END)
                        AS CUST_BAL_MIN         --合计余额_最小值
  ,(CASE WHEN b.DAYOFYEAR = ''1'' THEN a.PERIOD_ID
    ELSE
        (CASE WHEN NVL(a.CUST_BAL,0) - NVL(e.CUST_BAL_MIN,0) < 0 THEN a.PERIOD_ID ELSE e.CUST_BAL_MIN_DATE END) END)
                        AS CUST_BAL_MIN_DATE    --合计余额_最小值_日期
  ,(CASE WHEN b.DAYOFYEAR = ''1'' THEN a.CUST_BAL
    ELSE 
        ((NVL(e.CUST_BAL_AVG,0) * (b.DAYOFYEAR-1) + NVL(a.CUST_BAL,0)) / b.DAYOFYEAR) END)
                        AS CUST_BAL_AVG         --合计日平均余额
  ,c.CUST_BAL_AVG       AS CUST_BAL_AVG_CWS     --合计日平均余额_同期
  ,d.CUST_BAL_AVG       AS CUST_BAL_AVG_SQT     --合计日平均余额_上期

FROM MMAPDM.TMP_CUST_BAL_T a
LEFT JOIN  MMAPST.MID_CALENDAR b
ON a.PERIOD_ID = b.PERIOD_ID
--同期c
LEFT JOIN  MMAPDM.DM_CUST_BAL_STAT c
ON b.YEAR - 1 = c.YEAR
AND b.YEAR = c.freq_value
AND a.CUSTOMER_ID = c.CUSTOMER_ID
AND c.PROD_TYPE = a.PROD_TYPE
AND c.FREQ = ''Y''
--上期d
LEFT JOIN MMAPDM.DM_CUST_BAL_STAT d
ON b.YEAR = d.YEAR
AND b.YEAR - 1= d.freq_value
AND a.CUSTOMER_ID = d.CUSTOMER_ID
AND d.PROD_TYPE = a.PROD_TYPE
AND d.FREQ = ''Y''
--最值e
LEFT JOIN MMAPDM.DM_CUST_BAL_STAT e
ON TO_NUMBER(TO_CHAR(a.TX_DATE - 1,''yyyymmdd'')) = e.PERIOD_ID
AND b.YEAR = e.freq_value
AND a.CUSTOMER_ID = e.CUSTOMER_ID
AND e.PROD_TYPE = a.PROD_TYPE
AND e.FREQ = ''Y''
'
;
EXECUTE  IMMEDIATE DM_SQL;
COMMIT;
/*
    2. 删除前一天历史数据
*/
DELETE FROM MMAPDM.DM_CUST_BAL_STAT
WHERE       FREQ_VALUE IN (
                SELECT DISTINCT YEAR 
                FROM   MMAPST.MID_CALENDAR     
                WHERE  PERIOD_ID IN (SELECT DISTINCT PERIOD_ID
                                    FROM MMAPST.ST_CUST_BAL)                
            )
AND         PERIOD_ID IN (
                SELECT DISTINCT PERIOD_ID - 1
                FROM MMAPST.ST_CUST_BAL
            )
AND         FREQ = 'Y';



/*
    3. 插入当天数据
*/

DM_SQL:= 'INSERT INTO MMAPDM.DM_CUST_BAL_STAT
(
     ETL_DATE               --跑批日期(YYYYMMDD)                        
    ,TX_DATE                --数据日期(YYYYMMDD)
    ,PERIOD_ID              --日期(YYYYMMDD)
    ,CUSTOMER_ID            --客户号
    ,FREQ                   --频度（D\W\M\Q\Y）  
    ,YEAR                   --年份(YYYY)  
    ,FREQ_VALUE             --频度值(1\2\3\4)    
    ,FREQ_DIFF              --频度差(与更新日期的季度差值)   
    ,PROD_TYPE              --产品大类（活期、定期、基金、资产总额（人民币+外币）等）   
    ,CUST_BAL_LC            --余额  
    ,CUST_BAL_CWS_LC        --余额_同期      
    ,CUST_BAL_SQT_LC        --余额_上期        
    ,CUST_BAL_MAX_LC        --余额_最大值          
    ,CUST_BAL_MAX_DATE_LC   --余额_最大值_日期                 
    ,CUST_BAL_MIN_LC        --余额_最小值                  
    ,CUST_BAL_MIN_DATE_LC   --余额_最小值_日期                       
    ,CUST_BAL_AVG_LC        --日平均余额              
    ,CUST_BAL_AVG_CWS_LC    --日平均余额_同期                      
    ,CUST_BAL_AVG_SQT_LC    --日平均余额_上期                        
    ,CUST_BAL_FC            --外币余额                
    ,CUST_BAL_CWS_FC        --外币余额_同期
    ,CUST_BAL_SQT_FC        --外币余额_上期
    ,CUST_BAL_MAX_FC        --外币余额_最大值
    ,CUST_BAL_MAX_DATE_FC   --外币余额_最大值_日期
    ,CUST_BAL_MIN_FC        --外币余额_最小值
    ,CUST_BAL_MIN_DATE_FC   --外币余额_最小值_日期
    ,CUST_BAL_AVG_FC        --外币日平均余额
    ,CUST_BAL_AVG_CWS_FC    --外币日平均余额_同期
    ,CUST_BAL_AVG_SQT_FC    --外币日平均余额_上期
    ,CUST_BAL               --合计余额  
    ,CUST_BAL_CWS           --合计余额_同期      
    ,CUST_BAL_SQT           --合计余额_上期        
    ,CUST_BAL_MAX           --合计余额_最大值          
    ,CUST_BAL_MAX_DATE      --合计余额_最大值_日期                 
    ,CUST_BAL_MIN           --合计余额_最小值                  
    ,CUST_BAL_MIN_DATE      --合计余额_最小值_日期                       
    ,CUST_BAL_AVG           --合计日平均余额              
    ,CUST_BAL_AVG_CWS       --合计日平均余额_同期                      
    ,CUST_BAL_AVG_SQT       --合计日平均余额_上期 
    ,NEW_FLAG    
)
SELECT 
     ETL_DATE     
    ,TX_DATE      
    ,PERIOD_ID    
    ,CUSTOMER_ID
    ,''Y'' AS FREQ        --频度（D\W\M\Q\Y）
    ,YEAR
    ,FREQ_VALUE
    ,0 AS FREQ_DIFF
    ,PROD_TYPE  --产品大类（活期、定期、基金、资产总额（人民币
    ,CUST_BAL_LC         
    ,CUST_BAL_CWS_LC     
    ,CUST_BAL_SQT_LC     
    ,CUST_BAL_MAX_LC     
    ,CUST_BAL_MAX_DATE_LC
    ,CUST_BAL_MIN_LC     
    ,CUST_BAL_MIN_DATE_LC
    ,CUST_BAL_AVG_LC     
    ,CUST_BAL_AVG_CWS_LC 
    ,CUST_BAL_AVG_SQT_LC 
    ,CUST_BAL_FC
    ,CUST_BAL_CWS_FC    
    ,CUST_BAL_SQT_FC
    ,CUST_BAL_MAX_FC
    ,CUST_BAL_MAX_DATE_FC
    ,CUST_BAL_MIN_FC
    ,CUST_BAL_MIN_DATE_FC
    ,CUST_BAL_AVG_FC
    ,CUST_BAL_AVG_CWS_FC
    ,CUST_BAL_AVG_SQT_FC
    ,CUST_BAL
    ,CUST_BAL_CWS   
    ,CUST_BAL_SQT
    ,CUST_BAL_MAX
    ,CUST_BAL_MAX_DATE
    ,CUST_BAL_MIN
    ,CUST_BAL_MIN_DATE
    ,CUST_BAL_AVG
    ,CUST_BAL_AVG_CWS
    ,CUST_BAL_AVG_SQT   
    ,1 AS NEW_FLAG
FROM  MMAPDM.TMP_CUST_BAL_CAL'
;
EXECUTE  IMMEDIATE DM_SQL;
IO_ROW := IO_ROW+SQL%ROWCOUNT ;
COMMIT;

/*
    4. 更新频度差
*/

UPDATE  MMAPDM.DM_CUST_BAL_STAT 
SET     FREQ_DIFF = FREQ_DIFF + 1 
WHERE   FREQ = 'Y' 
AND     NEW_FLAG = '0'
AND     (SELECT DISTINCT a.DAYOFYEAR FROM MMAPST.ST_CUST_BAL b
          LEFT JOIN MMAPST.MID_CALENDAR a
          ON a.PERIOD_ID=b.PERIOD_ID) = 1;

/*
    5. 删除过期数据（周数据留存12期）
*/

DELETE FROM MMAPDM.DM_CUST_BAL_STAT
WHERE       FREQ_DIFF >= 2
AND         FREQ = 'Y'

;
/*
  6. 删除临时表
*/
DM_SQL:= 'DROP TABLE MMAPDM.TMP_CUST_BAL_T';                   
EXECUTE  IMMEDIATE DM_SQL;
COMMIT;

DM_SQL:= 'DROP TABLE MMAPDM.TMP_CUST_BAL_CAL';
EXECUTE  IMMEDIATE DM_SQL;
COMMIT;
/* 
    写入日志 
*/
    
    SELECT SYSDATE INTO V_END_TIMESTAMP   FROM dual;    -- 加载程序运行结束时间
    IO_STATUS := 0 ;
    VO_SQLERR := 'SUSSCESS';
    P_MMAPDM_WRITE_LOGS(PROCEDURE_NAME,IO_STATUS,IO_ROW,V_START_TIMESTAMP,V_END_TIMESTAMP,VO_SQLERR);
    COMMIT;
    
  EXCEPTION
    WHEN OTHERS THEN
  ROLLBACK ;
  IO_STATUS := 9 ;
  VO_SQLERR := SQLCODE ||  SQLERRM  ;
  SELECT SYSDATE INTO  V_END_TIMESTAMP  FROM dual;
  P_MMAPDM_WRITE_LOGS(PROCEDURE_NAME,IO_STATUS,IO_ROW,V_START_TIMESTAMP,V_END_TIMESTAMP,VO_SQLERR);
END P_DM_CUST_BAL_STAT;
