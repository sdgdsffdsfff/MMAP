CREATE OR REPLACE PROCEDURE "MMAPDM"."P_DM_DEP_TRANS_FLOW_HIS"
(VO_SQLERR OUT VARCHAR2)
AS
        TABLE_NAME VARCHAR2(125) :='DM_DEP_TRANS_FLOW_HIS'; -- 表名
    DM_SQL VARCHAR2(20000); -- 存放SQL语句
    IO_ROW INTEGER;
    IO_STATUS INTEGER;
    V_START_TIMESTAMP TIMESTAMP;    -- 加载开始时间
    V_END_TIMESTAMP   TIMESTAMP;    -- 加载结束时间
    DM_TODAY NUMBER;        -- 数据日期"当日"
    TX_DATE NUMBER;
     
BEGIN
        /*针对同一天重复执行此存储过程的异常情况，插入前先删除"当日"数据*/
    SELECT TO_NUMBER(TO_CHAR((SYSDATE-1),'YYYYMMDD')) INTO DM_TODAY FROM dual;
    DELETE FROM DM_DEP_TRANS_FLOW_HIS WHERE TO_NUMBER(TO_CHAR(TX_DATE,'YYYYMMDD')) = DM_TODAY;    -- 删除开始日期为"当日"的数据
    COMMIT;
     
        /*插入当日交易数据*/
        DM_SQL := 'INSERT INTO DM_DEP_TRANS_FLOW_HIS
    (
        ETL_DATE,
        TX_DATE,
        PERIOD_ID,
        TRANS_DT,
        TRANS_TM,
        SER_NO,
        ISER_NO,
        OPER_ORG,
        CUSTOMER_ID,
        ACCOUNT_ID,
        ACCTTYP_ID,
        BUSSTYP_ID,
        TRANSCDE_ID,
        TREATMENTCODE,
        TRANSDIR_FG,
        TRANSST_ID,
        TRANSBR_ID,
        CHN_ID,
        PRODCDE_ID,
        TRANS_AMT1,
        AFTRANS_AMT1,
        TRANSCURR_ID,
        TRANS_AMT,
        FEECURR_ID,
        TRANS_FEE,
        OACCTCURR_ID,
        REMARK,
        PURPDESC,
        IMGIDX_NO,
        IMGTYP_ID,
        EFFECT_DT,
        EFFECT_TM,
        CHCK_NO,
        TRANSACCTTYP_ID,
        TRANSACCTCURR_ID,
        TRANS_BAL,
        TRANSACCT_ORG,
        CARD_ORG,
        SCNTRY_ID,
        SENTITY_ID,
        MERC_ID,
        REVCARD_NM,
        MERCTYP_ID,
        MERC_NM,
        SOURCEGRP
    )
    SELECT
        ETL_DATE,
        TX_DATE,
        PERIOD_ID,
        TRANS_DT,
        TRANS_TM,
        SER_NO,
        ISER_NO,
        OPER_ORG,
        CUSTOMER_ID,
        ACCOUNT_ID,
        ACCTTYP_ID,
        BUSSTYP_ID,
        TRANSCDE_ID,
        TREATMENTCODE,
        TRANSDIR_FG,
        TRANSST_ID,
        TRANSBR_ID,
        CHN_ID,
        PRODCDE_ID,
        TRANS_AMT1,
        AFTRANS_AMT1,
        TRANSCURR_ID,
        TRANS_AMT,
        FEECURR_ID,
        TRANS_FEE,
        OACCTCURR_ID,
        REMARK,
        PURPDESC,
        IMGIDX_NO,
        IMGTYP_ID,
        EFFECT_DT,
        EFFECT_TM,
        CHCK_NO,
        TRANSACCTTYP_ID,
        TRANSACCTCURR_ID,
        TRANS_BAL,
        TRANSACCT_ORG,
        CARD_ORG,
        SCNTRY_ID,
        SENTITY_ID,
        MERC_ID,
        REVCARD_NM,
        MERCTYP_ID,
        MERC_NM,
        SOURCEGRP
        FROM MMAPST.ST_DEP_TRANS_FLOW';
        EXECUTE IMMEDIATE DM_SQL;
    COMMIT;
     
    /*写入日志信息*/
    IO_ROW := SQL%ROWCOUNT ;
    SELECT SYSDATE INTO V_START_TIMESTAMP FROM dual;    -- 加载程序运行开始时间
    SELECT SYSDATE INTO V_END_TIMESTAMP   FROM dual;    -- 加载程序运行结束时间
     
    IO_STATUS := 0 ;
    VO_SQLERR := 'SUSSCESS';
    P_MMAPDM_WRITE_LOGS(TABLE_NAME,IO_STATUS,IO_ROW,V_START_TIMESTAMP,V_END_TIMESTAMP,VO_SQLERR);
    COMMIT;
    EXCEPTION
      WHEN OTHERS THEN
    ROLLBACK ;
    IO_STATUS := 9 ;
    VO_SQLERR := SQLCODE || SQLERRM ;
    SELECT SYSDATE INTO V_END_TIMESTAMP FROM dual;
    P_MMAPDM_WRITE_LOGS(TABLE_NAME,IO_STATUS,IO_ROW,V_START_TIMESTAMP,V_END_TIMESTAMP,VO_SQLERR);
END;