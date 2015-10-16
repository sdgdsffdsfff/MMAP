CREATE OR REPLACE PROCEDURE "MMAPDM"."P_DM_CUST_BAL_STAT_FREQ"
(TABLE_NAME IN VARCHAR2, --表名
  FREQ IN VARCHAR2, --频度
  FREQ_VALUE IN VARCHAR2, --频度值
  DAY_OF_FREQ IN VARCHAR2, --频度中的第几天
  FREQ_HIS IN NUMBER, --频度留存历史期数
  DM_TODAY IN NUMBER,        -- 数据日期"当日"
  DM_YESTERDAY IN NUMBER, -- 数据日期"上一日"
  IO_ROW OUT INTEGER,
  IO_STATUS OUT INTEGER
)

AS

  PROCEDURE_NAME VARCHAR2(125) :='P_'||TABLE_NAME;  -- 存储过程名(修改)
  DM_TABLE_NAME VARCHAR2(125) := TABLE_NAME;  -- 表名(修改)

  IO_SQLERR VARCHAR2(2000);

  DM_SQL VARCHAR2(30000); -- the variable to loading the SQL statment

  ST_ROW INTEGER;  --源数据条数


  V_START_TIMESTAMP TIMESTAMP;    --the start time of procedures
  V_END_TIMESTAMP   TIMESTAMP;    --the end time of procedures

  V_COUNT NUMBER;  --计数变量
  V_COMMITNUM CONSTANT NUMBER :=500000;--一次提交记录数（默认一百万）

  --定义一个游标数据类型
  TYPE emp_cursor_type IS REF CURSOR;
  --声明一个游标变量
  c1 EMP_CURSOR_TYPE;
  --声明记录变量
  V_DM_CUST_BAL_W_STAT DM_CUST_BAL_W_STAT%ROWTYPE;
  V_DM_CUST_BAL_M_STAT DM_CUST_BAL_M_STAT%ROWTYPE;
  V_DM_CUST_BAL_Q_STAT DM_CUST_BAL_Q_STAT%ROWTYPE;
  V_DM_CUST_BAL_Y_STAT DM_CUST_BAL_Y_STAT%ROWTYPE;

BEGIN

  IO_ROW := 0;  --插入条数

  SELECT SYSDATE INTO V_START_TIMESTAMP FROM dual;    -- 加载程序运行开始时间

  --恢复'上一日'数据。
  --查询DM层表中是否有"今日"数据，是否为重跑。
  DM_SQL := ' SELECT COUNT(1) FROM MMAPDM.'||TABLE_NAME||' WHERE PERIOD_ID='||DM_TODAY;
  EXECUTE  IMMEDIATE DM_SQL  INTO ST_ROW;
  IF ST_ROW>0
    --如果为重跑数据，则将数据恢复为'前一日'数据状态
  THEN
    --1.删除'今日'数据
    DM_SQL :='DELETE FROM MMAPDM.'||TABLE_NAME||' WHERE PERIOD_ID='||DM_TODAY;
    EXECUTE IMMEDIATE DM_SQL;
    IO_ROW := IO_ROW+SQL%ROWCOUNT ;
    COMMIT;
    --2.恢复频度差
    DM_SQL :='UPDATE MMAPDM.'||TABLE_NAME||' SET FREQ_DIFF = FREQ_DIFF - 1
        WHERE (SELECT '||DAY_OF_FREQ||' FROM MMAPDM.MID_CALENDAR WHERE PERIOD_ID='||DM_TODAY||')=1
      ';
    EXECUTE IMMEDIATE DM_SQL;
    IO_ROW := IO_ROW+SQL%ROWCOUNT ;
    COMMIT;
    --3.插入昨日数据
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
    IO_ROW := IO_ROW+SQL%ROWCOUNT ;
    COMMIT;
    END IF;

    --备份"上日"数据
    --查询DM层表中是否有"上一日"数据，是否为重跑。
    DM_SQL := ' SELECT COUNT(1) FROM MMAPDM.'||TABLE_NAME||' WHERE PERIOD_ID='||DM_YESTERDAY;
    EXECUTE  IMMEDIATE DM_SQL  INTO ST_ROW;
    IF ST_ROW>0
    THEN
      DM_SQL :='DELETE FROM MMAPDM.DM_CUST_BAL_STAT_PRE WHERE FREQ='''||FREQ||'''' ;   -- 删除备份表中月频度数据
      EXECUTE IMMEDIATE DM_SQL;
      COMMIT;
      DM_SQL :='INSERT INTO MMAPDM.DM_CUST_BAL_STAT_PRE SELECT * FROM MMAPDM.'||TABLE_NAME||' WHERE PERIOD_ID='||DM_YESTERDAY ;   -- 备份表中上日数据
      EXECUTE IMMEDIATE DM_SQL;
      IO_ROW :=IO_ROW+ SQL%ROWCOUNT ;
      COMMIT;
    END IF;

    --1.更新频度差
    DM_SQL :='UPDATE MMAPDM.'||TABLE_NAME||' SET FREQ_DIFF = FREQ_DIFF + 1
        WHERE (SELECT '||DAY_OF_FREQ||' FROM MMAPDM.MID_CALENDAR WHERE PERIOD_ID='||DM_TODAY||')=1
      ';
    EXECUTE IMMEDIATE DM_SQL;
    IO_ROW := IO_ROW+SQL%ROWCOUNT ;
    COMMIT;

    --2. 建立临时表

    -----取前一天数据-----
    SELECT COUNT(1) INTO ST_ROW FROM USER_TABLES WHERE TABLE_NAME = 'TMP_CUST_BAL_CAL';
    IF ST_ROW >0
    THEN
      DM_SQL := 'DROP TABLE MMAPDM.TMP_CUST_BAL_CAL PURGE';
    EXECUTE  IMMEDIATE DM_SQL;
    END IF;

    DM_SQL:= 'CREATE TABLE MMAPDM.TMP_CUST_BAL_CAL AS
      SELECT
         A.ETL_DATE
        ,A.TX_DATE
        ,A.PERIOD_ID
        ,B.YEAR
        ,B.'||DAY_OF_FREQ||'
        ,B.'||FREQ_VALUE||' AS FREQ_VALUE
        ,'''||FREQ||'''  AS FREQ
        ,0 AS FREQ_DIFF
        ,A.CUSTOMER_ID
        ,A.PROD_TYPE AS PROD_TYPE
        ,A.CUST_BAL_LC AS CUST_BAL_LC
        --,C.CUST_BAL_LC AS CUST_BAL_CWS_LC
        ,0 AS CUST_BAL_CWS_LC
        ,D.CUST_BAL_LC AS CUST_BAL_SQT_LC
        ,CASE WHEN E.CUSTOMER_ID IS NULL  THEN A.CUST_BAL_LC
          ELSE GREATEST(NVL(A.CUST_BAL_LC,0),NVL(E.CUST_BAL_MAX_LC,0))
         END AS CUST_BAL_MAX_LC
        ,CASE WHEN E.CUSTOMER_ID IS NULL  THEN A.PERIOD_ID
          ELSE (CASE WHEN NVL(A.CUST_BAL_LC,0) - NVL(E.CUST_BAL_MAX_LC,0) > 0
            THEN a.PERIOD_ID ELSE e.CUST_BAL_MAX_DATE_LC
                END)
         END AS CUST_BAL_MAX_DATE_LC
        ,CASE WHEN E.CUSTOMER_ID IS NULL  THEN A.CUST_BAL_LC
          ELSE LEAST(NVL(A.CUST_BAL_LC,0),NVL(E.CUST_BAL_MIN_LC,0))
         END AS CUST_BAL_MIN_LC
        ,CASE WHEN E.CUSTOMER_ID IS NULL  THEN A.PERIOD_ID
          ELSE (CASE WHEN NVL(A.CUST_BAL_LC,0) - NVL(E.CUST_BAL_MIN_LC,0) < 0 THEN a.PERIOD_ID ELSE e.CUST_BAL_MIN_DATE_LC END)
         END AS CUST_BAL_MIN_DATE_LC
        ,CASE WHEN E.CUSTOMER_ID IS NULL THEN A.CUST_BAL_LC
          ELSE ((NVL(E.CUST_BAL_AVG_LC,0) * (B.'||DAY_OF_FREQ||'-1) + NVL(a.CUST_BAL_LC,0)) / b.'||DAY_OF_FREQ||')
         END AS CUST_BAL_AVG_LC
        --,C.CUST_BAL_AVG_LC        AS CUST_BAL_AVG_CWS_LC
        ,0        AS CUST_BAL_AVG_CWS_LC
        ,D.CUST_BAL_AVG_LC        AS CUST_BAL_AVG_SQT_LC
        ,A.CUST_BAL_FC            AS CUST_BAL_FC
        --,C.CUST_BAL_FC           AS CUST_BAL_CWS_FC
        ,0           AS CUST_BAL_CWS_FC
        ,D.CUST_BAL_FC           AS CUST_BAL_SQT_FC
        ,CASE WHEN E.CUSTOMER_ID IS NULL  THEN A.CUST_BAL_FC
          ELSE GREATEST(NVL(A.CUST_BAL_FC,0),NVL(E.CUST_BAL_MAX_FC,0))
         END AS CUST_BAL_MAX_FC
        ,CASE WHEN E.CUSTOMER_ID IS NULL  THEN A.PERIOD_ID
          ELSE (CASE WHEN NVL(A.CUST_BAL_FC,0) - NVL(e.CUST_BAL_MAX_FC,0) > 0 THEN a.PERIOD_ID ELSE e.CUST_BAL_MAX_DATE_FC END)
         END AS CUST_BAL_MAX_DATE_FC
        ,CASE WHEN E.CUSTOMER_ID IS NULL  THEN A.CUST_BAL_FC
          ELSE LEAST(NVL(A.CUST_BAL_FC,0),NVL(E.CUST_BAL_MIN_FC,0))
         END AS CUST_BAL_MIN_FC
        ,CASE WHEN E.CUSTOMER_ID IS NULL  THEN A.PERIOD_ID
          ELSE (CASE WHEN NVL(A.CUST_BAL_FC,0) - NVL(e.CUST_BAL_MIN_FC,0) < 0 THEN a.PERIOD_ID ELSE e.CUST_BAL_MIN_DATE_FC END)
         END AS CUST_BAL_MIN_DATE_FC
        ,CASE WHEN E.CUSTOMER_ID IS NULL  THEN A.CUST_BAL_FC
          ELSE ((NVL(E.CUST_BAL_AVG_FC,0) * (B.'||DAY_OF_FREQ||'-1) + NVL(a.CUST_BAL_FC,0)) / b.'||DAY_OF_FREQ||')
         END AS CUST_BAL_AVG_FC
        --,C.CUST_BAL_AVG_FC    AS CUST_BAL_AVG_CWS_FC
        ,0    AS CUST_BAL_AVG_CWS_FC
        ,D.CUST_BAL_AVG_FC    AS  CUST_BAL_AVG_SQT_FC
        ,A.CUST_BAL                AS CUST_BAL
        --,C.CUST_BAL                AS CUST_BAL_CWS
        ,0                AS CUST_BAL_CWS
        ,D.CUST_BAL                AS CUST_BAL_SQT
        ,CASE WHEN E.CUSTOMER_ID IS NULL  THEN A.CUST_BAL
          ELSE GREATEST(NVL(A.CUST_BAL,0),NVL(E.CUST_BAL_MAX,0))
         END AS CUST_BAL_MAX
        ,CASE WHEN E.CUSTOMER_ID IS NULL  THEN A.PERIOD_ID
          ELSE (CASE WHEN NVL(A.CUST_BAL,0) - NVL(E.CUST_BAL_MAX,0) > 0 THEN a.PERIOD_ID ELSE e.CUST_BAL_MAX_DATE END)
         END AS CUST_BAL_MAX_DATE
        ,CASE WHEN E.CUSTOMER_ID IS NULL  THEN A.CUST_BAL
          ELSE LEAST(NVL(A.CUST_BAL,0),NVL(E.CUST_BAL_MIN,0))
         END AS CUST_BAL_MIN
        ,CASE WHEN E.CUSTOMER_ID IS NULL  THEN A.PERIOD_ID
          ELSE (CASE WHEN NVL(A.CUST_BAL,0) - NVL(E.CUST_BAL_MIN,0) < 0 THEN a.PERIOD_ID ELSE e.CUST_BAL_MIN_DATE END)
         END AS CUST_BAL_MIN_DATE
        ,CASE WHEN E.CUSTOMER_ID IS NULL THEN A.CUST_BAL
          ELSE ((NVL(E.CUST_BAL_AVG,0) * (B.'||DAY_OF_FREQ||'-1) + NVL(a.CUST_BAL,0)) / b.'||DAY_OF_FREQ||')
         END AS CUST_BAL_AVG
        --,C.CUST_BAL_AVG       AS CUST_BAL_AVG_CWS
        ,0       AS CUST_BAL_AVG_CWS
        ,D.CUST_BAL_AVG       AS CUST_BAL_AVG_SQT     --合计日平均余额_上期
      FROM MMAPDM.TMP_CUST_BAL_T A
      LEFT JOIN  MMAPDM.MID_CALENDAR B
      ON A.PERIOD_ID = B.PERIOD_ID
      /*
      --同期c，暂不计算同期数据
      LEFT JOIN  MMAPDM.'||TABLE_NAME||' C
      ON B.YEAR - 1 = C.YEAR
      AND B.'||FREQ_VALUE||' = C.FREQ_VALUE
      AND A.CUSTOMER_ID = C.CUSTOMER_ID
      AND C.PROD_TYPE = A.PROD_TYPE
      */
      --上期d，取上一统计期数据
      LEFT JOIN MMAPDM.'||TABLE_NAME||' D
      ON D.FREQ_DIFF = 1
      AND A.CUSTOMER_ID = D.CUSTOMER_ID
      AND D.PROD_TYPE = A.PROD_TYPE
      --最值e，取本统计期内数据
      LEFT JOIN MMAPDM.'||TABLE_NAME||' E
      ON B.YEAR = E.YEAR
      AND B.'||FREQ_VALUE||' = E.FREQ_VALUE
      AND A.CUSTOMER_ID = E.CUSTOMER_ID
      AND E.PROD_TYPE = A.PROD_TYPE
      ';
    EXECUTE IMMEDIATE DM_SQL;
    COMMIT;

    --删除本统计周期旧数据，及超期历史数据
    DM_SQL:= 'DELETE FROM MMAPDM.'||TABLE_NAME||' WHERE FREQ_DIFF =0 OR FREQ_DIFF>='||FREQ_HIS;
    EXECUTE IMMEDIATE DM_SQL;
    IO_ROW := IO_ROW+SQL%ROWCOUNT ;
    COMMIT;
    --插入当天数据
    BEGIN
      DM_SQL:='
      SELECT
           ETL_DATE
          ,TX_DATE
          ,PERIOD_ID
          ,FREQ
          ,YEAR
          ,FREQ_VALUE
          ,0 AS FREQ_DIFF
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
          ,0 AS NEW_FLAG
      FROM  MMAPDM.TMP_CUST_BAL_CAL
      ';
    --计数器初始化
    V_COUNT  := 0;

    --批量插入当日数据
    OPEN C1 FOR DM_SQL;
      LOOP
        IF DM_TABLE_NAME='DM_CUST_BAL_W_STAT'
        THEN
          FETCH C1 INTO V_DM_CUST_BAL_W_STAT;
          EXIT WHEN C1%NOTFOUND;
          INSERT INTO DM_CUST_BAL_W_STAT VALUES V_DM_CUST_BAL_W_STAT;
          IO_ROW := IO_ROW+SQL%ROWCOUNT ;

        ELSIF DM_TABLE_NAME='DM_CUST_BAL_M_STAT'
        THEN
          FETCH C1 INTO V_DM_CUST_BAL_M_STAT;
          EXIT WHEN C1%NOTFOUND;
          INSERT INTO DM_CUST_BAL_M_STAT VALUES V_DM_CUST_BAL_M_STAT;
          IO_ROW := IO_ROW+SQL%ROWCOUNT ;

        ELSIF DM_TABLE_NAME='DM_CUST_BAL_Q_STAT'
        THEN
          FETCH C1 INTO V_DM_CUST_BAL_Q_STAT;
          EXIT WHEN C1%NOTFOUND;
          INSERT INTO DM_CUST_BAL_Q_STAT VALUES V_DM_CUST_BAL_Q_STAT;
          IO_ROW := IO_ROW+SQL%ROWCOUNT ;

        ELSIF DM_TABLE_NAME='DM_CUST_BAL_Y_STAT'
        THEN
          FETCH C1 INTO V_DM_CUST_BAL_Y_STAT;
          EXIT WHEN C1%NOTFOUND;
          INSERT INTO DM_CUST_BAL_Y_STAT VALUES V_DM_CUST_BAL_Y_STAT;
          IO_ROW := IO_ROW+SQL%ROWCOUNT ;

        ELSE EXIT;
        END IF;
        V_COUNT := V_COUNT + 1;
        IF (V_COMMITNUM>0 AND (MOD(V_COUNT, V_COMMITNUM)) = 0) THEN
          COMMIT;
        END IF;

    END LOOP;
    COMMIT;
    CLOSE C1;
  END;

    /*
        写入日志
    */

    SELECT SYSDATE INTO V_END_TIMESTAMP   FROM dual;    -- 加载程序运行结束时间
    IO_STATUS := 0 ;
    IO_SQLERR := 'SUSSCESS';
    P_MMAPDM_WRITE_LOGS(PROCEDURE_NAME,IO_STATUS,IO_ROW,V_START_TIMESTAMP,V_END_TIMESTAMP,IO_SQLERR);
    COMMIT;

    EXCEPTION
      WHEN OTHERS THEN
    ROLLBACK ;
    IO_STATUS := 9 ;
    P_DM_CUST_BAL_STAT_ROLLBACK(TABLE_NAME,DM_TODAY,DM_YESTERDAY,FREQ);
    IO_SQLERR := SQLCODE ||  SQLERRM  ;
    SELECT SYSDATE INTO  V_END_TIMESTAMP  FROM dual;
    P_MMAPDM_WRITE_LOGS(PROCEDURE_NAME,IO_STATUS,IO_ROW,V_START_TIMESTAMP,V_END_TIMESTAMP,IO_SQLERR);
END P_DM_CUST_BAL_STAT_FREQ;
