TRUNCATE TABLE MMAPST.ST_ACCOUNT_AMT;
INSERT INTO MMAPST.ST_ACCOUNT_AMT
(
ETL_DATE
,TX_DATE
,PERIOD_ID
,CUSTOMER_ID
,ACCOUNT_ID
,CURR_CODE
,ACCT_BRANCH
,TOPPROD_CODE
,PROD_CODE
,ACC_BAL1
,ACC_BAL
,AVACC_MBAL1
,AVACC_MBAL
,AVACC_YBAL1
,AVACC_YBAL
,N_DCRTRANS
,N_DDRTRANS
,TRANS_DCRAMT1
,TRANS_DDRAMT1
,TRANS_DCRAMT
,TRANS_DDRAMT
,N_MCRTRANS
,N_MDRTRANS
,TRANS_MCRAMT1
,TRANS_MDRAMT1
,TRANS_MCRAMT
,TRANS_MDRAMT
,N_YCRTRANS
,N_YDRTRANS
,TRANS_YCRAMT1
,TRANS_YDRAMT1
,TRANS_YCRAMT
,TRANS_YDRAMT
,N_AVMCRTRANS
,N_AVMDRTRANS
,AVTRANS_MCRAMT1
,AVTRANS_MDRAMT1
,AVTRANS_MCRAMT
,AVTRANS_MDRAMT
,N_AVYCRTRANS
,N_AVYDRTRANS
,AVTRANS_YCRAMT1
,AVTRANS_YDRAMT1
,AVTRANS_YCRAMT
,AVTRANS_YDRAMT

)
SELECT 
TO_NUMBER(TO_CHAR(SYSDATE,'YYYYMMDD'))
,A.TX_DT
,TO_CHAR(A.TX_DT,'YYYYMMDD')
,TRIM(A.CIF_ORG）
,TRIM(A.ACC_ORG）
,TRIM(A.CURR_ID）
,TRIM(A.BRNH_ID）
,TRIM(A.TOPPROD_ID）
,TRIM(A.PRODTYP_ID）
,A.ACC_BAL1
,A.ACC_BAL
,A.AVACC_MBAL1
,A.AVACC_MBAL
,A.AVACC_YBAL1
,A.AVACC_YBAL
,A.N_DCRTRANS
,A.N_DDRTRANS
,A.TRANS_DCRAMT1
,A.TRANS_DDRAMT1
,A.TRANS_DCRAMT
,A.TRANS_DDRAMT
,A.N_MCRTRANS
,A.N_MDRTRANS
,A.TRANS_MCRAMT1
,A.TRANS_MDRAMT1
,A.TRANS_MCRAMT
,A.TRANS_MDRAMT
,N_YCRTRANS
,N_YDRTRANS
,TRANS_YCRAMT1
,TRANS_YDRAMT1
,TRANS_YCRAMT
,TRANS_YDRAMT
,N_AVMCRTRANS
,N_AVMDRTRANS
,AVTRANS_MCRAMT1
,AVTRANS_MDRAMT1
,AVTRANS_MCRAMT
,AVTRANS_MDRAMT
,N_AVYCRTRANS
,N_AVYDRTRANS
,AVTRANS_YCRAMT1
,AVTRANS_YDRAMT1
,AVTRANS_YCRAMT
,AVTRANS_YDRAMT
FROM MMAPDMMKT.DMMKT_ACLVLAMT A
INNER JOIN MMAPDMMKT.DMMKT_CSMAST B
ON TRIM(A.CIF_ORG)=TRIM(B.CIF_ORG)
AND TRIM(B.CIFTYP_ID)='I';

COMMIT;
