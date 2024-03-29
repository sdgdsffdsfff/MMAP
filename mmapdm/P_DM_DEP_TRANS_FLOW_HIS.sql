CREATE OR REPLACE PROCEDURE "MMAPDM"."P_DM_DEP_TRANS_FLOW_HIS"
(IO_STATUS OUT INTEGER,IO_SQLERR OUT VARCHAR2)
AS
  TABLE_NAME VARCHAR2(125) :='DM_DEP_TRANS_FLOW_HIS'; -- 表名
  PROCEDURE_NAME VARCHAR2(125) :='P_'||TABLE_NAME;  -- 存储过程名(修改)

  DM_SQL VARCHAR2(20000); -- 存放SQL语句

  IO_ROW INTEGER;  --插入条数
  ST_ROW INTEGER;  --源数据条数

  V_ETL_DATE NUMBER;  -- 跑批日期
  V_START_TIMESTAMP TIMESTAMP;    -- 加载开始时间
  V_END_TIMESTAMP   TIMESTAMP;    -- 加载结束时间

  DM_TODAY NUMBER;        -- 数据日期"当日"
  TX_DATE NUMBER;

    V_COUNT NUMBER;  --计数变量
    V_COMMITNUM CONSTANT NUMBER :=500000;--一次提交记录数（默认一百万）

  --定义一个游标数据类型
  TYPE emp_cursor_type IS REF CURSOR;
  --声明一个游标变量
  c1 EMP_CURSOR_TYPE;
  --声明记录变量
  V_DM_DEP_TRANS_FLOW_HIS DM_DEP_TRANS_FLOW_HIS%ROWTYPE;

BEGIN
  SELECT SYSDATE INTO V_START_TIMESTAMP FROM dual;    -- 加载程序运行开始时间

  SELECT TO_NUMBER(TO_CHAR((SYSDATE),'YYYYMMDD')) INTO V_ETL_DATE FROM dual;  -- 取系统日期作为跑批日期
  SELECT TX_DATE INTO DM_TODAY FROM MMAPST.ST_SYSTEM_DATE;  -- 取数据日期

  --查询ST层表中是否有'当日'数据
  SELECT COUNT(1) INTO ST_ROW FROM MMAPST.ST_DEP_TRANS_FLOW WHERE PERIOD_ID=DM_TODAY;

  --如果ST层有"当日"数据，则进行数据抽取，否则保持DM层现有数据不变。
  IF ST_ROW>0
  THEN
    /*针对同一天重复执行此存储过程的异常情况，插入前先删除"当日"数据*/
    DELETE FROM MMAPDM.DM_DEP_TRANS_FLOW_HIS WHERE PERIOD_ID = DM_TODAY;    -- 删除"当日"数据
    COMMIT;

    BEGIN
        --获取取数SQL
        DM_SQL := 'SELECT
            '|| V_ETL_DATE ||'
            ,A.TX_DATE
            ,A.PERIOD_ID
            ,TRANS_DT
            ,TRANS_TM
            ,SER_NO
            ,ISER_NO
            ,OPER_ORG
            ,CUSTOMER_ID
            ,ACCOUNT_ID
            ,ACCTTYP_ID
            ,BUSSTYP_ID
            ,TRANSCDE_ID
            ,TREATMENT_CODE
            ,TRANSDIR_FG
            ,TRANSST_ID
            ,TRANSBR_ID
            ,CHN_ID
            ,PRODCDE_ID
            ,B.PROD_TYPE_CODE
            ,TRANS_AMT1
            ,AFTRANS_AMT1
            ,TRANSCURR_ID
            ,TRANS_AMT
            ,FEECURR_ID
            ,TRANS_FEE
            ,OACCTCURR_ID
            ,REMARK
            ,PURPDESC
            ,IMGIDX_NO
            ,IMGTYP_ID
            ,EFFECT_DT
            ,EFFECT_TM
            ,CHCK_NO
            ,TRANSACCTTYP_ID
            ,TRANSACCTCURR_ID
            ,TRANS_BAL
            ,TRANSACCT_ORG
            ,CARD_ORG
            ,SCNTRY_ID
            ,SENTITY_ID
            ,MERC_ID
            ,REVCARD_NM
            ,MERCTYP_ID
            ,MERC_NM
            ,SOURCEGRP
          FROM  MMAPST.ST_DEP_TRANS_FLOW A
          LEFT  JOIN MMAPDM.DM_PRODUCT B
          ON  A.PRODCDE_ID=B.PROD_CODE';
        -- 计数器初始化
        V_COUNT  := 0;
        --批量插入当日数据
        OPEN C1 FOR DM_SQL;
        LOOP
            FETCH C1 INTO V_DM_DEP_TRANS_FLOW_HIS;
            EXIT WHEN C1%NOTFOUND;
            INSERT INTO DM_DEP_TRANS_FLOW_HIS VALUES V_DM_DEP_TRANS_FLOW_HIS;
            IO_ROW := IO_ROW+SQL%ROWCOUNT;
            V_COUNT := V_COUNT + 1;
            IF MOD(V_COUNT, V_COMMITNUM) = 0 THEN
              COMMIT;
            END IF;

            END LOOP;
        COMMIT;
        CLOSE C1;
    END;

END IF;


  /*写入日志信息*/
  SELECT SYSDATE INTO V_END_TIMESTAMP   FROM dual;    -- 加载程序运行结束时间
  IO_STATUS := 0 ;
  IO_SQLERR := 'SUSSCESS';
  P_MMAPDM_WRITE_LOGS(PROCEDURE_NAME,IO_STATUS,IO_ROW,V_START_TIMESTAMP,V_END_TIMESTAMP,IO_SQLERR);
  COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
  ROLLBACK ;
  IO_STATUS := 9 ;
  IO_SQLERR := SQLCODE || SQLERRM ;
  SELECT SYSDATE INTO V_END_TIMESTAMP FROM dual;
  P_MMAPDM_WRITE_LOGS(PROCEDURE_NAME,IO_STATUS,IO_ROW,V_START_TIMESTAMP,V_END_TIMESTAMP,IO_SQLERR);
END P_DM_DEP_TRANS_FLOW_HIS;
