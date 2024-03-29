CREATE OR REPLACE PROCEDURE "MMAPDM"."P_DM_ACCOUNT_DEP"
(IO_STATUS OUT INTEGER,IO_SQLERR OUT VARCHAR2)
AS
	TABLE_NAME VARCHAR2(125) :='ACCOUNT_DEP';  -- 表名(修改)
BEGIN
/*
--编辑取数SQL
DELETE MMAPDM.MMAPDM_PROC_SQL WHERE TABLE_NAME='DM_ACCOUNT_DEP';
COMMIT;
INSERT INTO MMAPDM.MMAPDM_PROC_SQL(TABLE_NAME,DMSQL,PROC_NAME) VALUES('DM_ACCOUNT_DEP','
      SELECT
        TO_NUMBER(TO_CHAR((SYSDATE),''YYYYMMDD'')),
        DA.TX_DATE,
				DA.PERIOD_ID,
				DA.CUSTOMER_ID,
				DA.ACCOUNT_ID,
				DA.CURR_CODE,
				DA.ACCT_STATUS_CODE,
        DA.OPEN_BRANCH,
        DA.OPEN_DATE,
        DA.ACCT_BAL,
        DA.ACCT_TYPE,
        DA.OFFCR_ID,
        DA.STMAINT_DT,
        DA.STMAINT_TM,
        DA.MAINTBUSS_ID,
        B.PROD_TYPE_CODE,
        DA.END_DATE,
        DA.PROD_ORG,
        DA.PRODTYP_ID,
        DA.PAYSTMTDIS_NO,
        DA.VALUE_DT,
        DA.PAYSTMTDIS_ID,
        DA.EXCH_RATE,
        DA.GLGRP_ID,
        DA.N_BAL,
        DA.SCORE_BAL,
        DA.MGMT_ID,
        DA.CKTYP_ID,
        DA.INTCAL_ID,
        DA.REDEMP_DT,
        DA.YRBASE_ID,
        DA.RENEW_FG,
        DA.WDISP_ID,
        DA.DY_AGGL,
        DA.PMTFRQ_ID,
        DA.RECEIPT_ID,
        DA.TERMTYP_ID,
        DA.INT_RATE,
        DA.LEFTINT_AMT,
        DA.MTPRN_AMT,
        DA.TW_AMTCR,
        DA.TW_AMTDR,
        DA.ACCGRP_ORG,
        DA.HOLD_ID,
        DA.TD_ID,
        DA.SCHED_AMT,
        DA.FXTYP_FG,
        DA.N_RENEW,
        DA.RPRICEFREQ,
        DA.RPRICEFREQ_ID,
        DA.HOLD_AMT,
        DA.CRTACC_AMT,
        DA.PEN_AMT,
        DA.THIRDPART_ORG,
        DA.EFF_DT,
        DA.EFF_TM
      FROM MMAPST.ST_ACCOUNT_DEP DA
      LEFT JOIN MMAPDM.DM_PRODUCT B
      ON B.PROD_CODE = DA.PRODTYP_ID','P_DM_ACCOUNT_DEP')
;
COMMIT;
*/
    P_DM_FULL(TABLE_NAME,100000,IO_STATUS,IO_SQLERR);
END P_DM_ACCOUNT_DEP;
