CREATE OR REPLACE PROCEDURE P_DM_CUST_BAL_STAT_ROLLBACK (
    TABLE_NAME IN VARCHAR2, --表名
    DM_TODAY IN NUMBER,--当日
    DM_YESTERDAY IN NUMBER, -- 数据日期"上一日"
    FREQ IN VARCHAR2 --频度  
   ) 
   AS
       DM_SQL VARCHAR2(20000);
   BEGIN
     DM_SQL :='UPDATE MMAPDM.'||TABLE_NAME||' SET FREQ_DIFF = FREQ_DIFF - 1
        WHERE (SELECT DAYOFWEEK FROM MMAPST.MID_CALENDAR WHERE PERIOD_ID='||DM_TODAY||')=1
      ';
    EXECUTE IMMEDIATE DM_SQL;
    COMMIT;
    DM_SQL:= 'INSERT INTO MMAPDM.'||TABLE_NAME||'
    (
         ETL_DATE               --跑批日期(YYYYMMDD)
        ,TX_DATE                --数据日期(YYYYMMDD)
        ,PERIOD_ID              --日期(YYYYMMDD)
        ,FREQ                   --频度（D\W\M\Q\Y）
        ,YEAR                   --年份(YYYY)
        ,FREQ_VALUE             --频度值(1\2\3\4)
        ,FREQ_DIFF              --频度差(与更新日期的季度差值)
        ,CUSTOMER_ID            --客户号
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
    )
    SELECT
         ETL_DATE
        ,TX_DATE
        ,PERIOD_ID
        ,FREQ
        ,YEAR
        ,FREQ_VALUE
        ,FREQ_DIFF
        ,CUSTOMER_ID
        ,PROD_TYPE
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
    FROM  MMAPDM.DM_CUST_BAL_STAT_PRE
    WHERE FREQ='''||FREQ||'''
    AND PERIOD_ID='||DM_YESTERDAY
    ;
    EXECUTE  IMMEDIATE DM_SQL;
    COMMIT;

   END P_DM_CUST_BAL_STAT_ROLLBACK;
/
