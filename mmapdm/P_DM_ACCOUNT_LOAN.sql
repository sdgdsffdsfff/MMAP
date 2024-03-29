CREATE OR REPLACE PROCEDURE "MMAPDM"."P_DM_ACCOUNT_LOAN"
(IO_STATUS OUT INTEGER,IO_SQLERR OUT VARCHAR2)
AS

  TABLE_NAME VARCHAR2(125) :='ACCOUNT_LOAN';  -- 表名(修改)
BEGIN
/*
--编辑取数SQL
DELETE MMAPDM.MMAPDM_PROC_SQL WHERE TABLE_NAME='DM_ACCOUNT_LOAN';
COMMIT;
INSERT INTO MMAPDM.MMAPDM_PROC_SQL(TABLE_NAME,DMSQL,PROC_NAME) VALUES('DM_ACCOUNT_LOAN','
      SELECT
        TO_NUMBER(TO_CHAR((SYSDATE),''YYYYMMDD'')),
				DA.TX_DATE,
				DA.PERIOD_ID,
				DA.CUSTOMER_ID,
        DA.ACCOUNT_ID,
        DA.CURR_CODE,
        DA.ACCT_STATUS,
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
        DA.INT_RATE,
        DA.BUSSTYP_ID,
        DA.INVT_ORG,
        DA.RATING_ID,
        DA.SIC_ID,
        DA.LNPURPS_ID,
        DA.SETTAVGCOSTYST,
        DA.AAPP_ORG,
        DA.AACC_ORG,
        DA.ACUSTOMER_ID,
        DA.BAILACC_ORG,
        DA.DISACC_ORG,
        DA.CAR_ID,
        DA.BUSTYP_ID,
        DA.LNTYP_ID,
        DA.MASIND_ID,
        DA.MTHDCOL_ID,
        DA.DISC_DT,
        DA.W_INSPRE1,
        DA.W_SUBSD,
        DA.TERMTYP_ID,
        DA.CLOS_DT,
        DA.FIRSTDIS_DT,
        DA.FIRSTPAY_DT,
        DA.FULLDIS_DT,
        DA.NDUE_DT,
        DA.WO_DT,
        DA.RATEFFET_DT,
        DA.PMT_ID,
        DA.RATEFQ_ID,
        DA.RATEFQT_ID,
        DA.INTFREQ_ID,
        DA.INTPRQ_ID,
        DA.INTPFQT_ID,
        DA.YRBASE_ID,
        DA.RAT_FEE,
        DA.RAT_LCHRG,
        DA.AGNFEE,
        DA.PRNDIS_AMT,
        DA.BAIL_AMT,
        DA.RPRICEMTHD_ID,
        DA.W_CBAL,
        DA.LC_NO,
        DA.ACCFEECAT_ID
           FROM MMAPST.ST_ACCOUNT_LOAN DA
      LEFT JOIN MMAPDM.DM_PRODUCT B
      ON B.PROD_CODE = DA.PRODTYP_ID','P_DM_ACCOUNT_LOAN')
;
COMMIT;
*/
   -- P_DM_FULL(TABLE_NAME,IO_STATUS,IO_SQLERR);
    P_DM_FULL(TABLE_NAME,10000,IO_STATUS,IO_SQLERR);
END P_DM_ACCOUNT_LOAN;
