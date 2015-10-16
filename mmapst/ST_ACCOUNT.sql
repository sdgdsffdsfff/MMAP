﻿TRUNCATE TABLE MMAPST.ST_ACCOUNT;
INSERT INTO MMAPST.ST_ACCOUNT
(
ETL_DATE
,TX_DATE
,PERIOD_ID
,CUSTOMER_ID
,ACCOUNT_ID
,ACCT_STATUS
,THIRD_ACCT_ID
,NATURE_ID
,STSCHG_DATE
,STMAINT_DATE
,ENTRS_BRANCH
,ACCAPP_DATE
,CHKACC_ID
,LMAINT_DATE
,ACCT_TYPE
,OPEN_DATE
,END_DATE
,OPEN_BRANCH
)
SELECT 
TO_NUMBER(TO_CHAR(SYSDATE,'YYYYMMDD'))
,A.TX_DT
,TO_CHAR(A.TX_DT,'YYYYMMDD')
,TRIM(A.CIF_ORG     )
,TRIM(A.ACC_ORG     )
,TRIM(A.ACCSTS_ID   )
,TRIM(A.TERTACC_ORG )
,TRIM(A.NATURE_ID   )
,A.STSCHG_DT
,A.STMAINT_DT
,TRIM(A.ENTRSBRNH_ID)
,A.ACCAPP_DT 
,TRIM(A.CHKACC_ID   )
,A.LMAINT_DT   
,TRIM(A.ACCTYP_ORG  )
,A.DEAL_DT     
,NULL
,TRIM(A.BRNH_ID     )
FROM MMAPDMMKT.DMMKT_CSACCREL A
INNER JOIN MMAPDMMKT.DMMKT_CSMAST B
ON TRIM(A.CIF_ORG)=TRIM(B.CIF_ORG)
AND TRIM(B.CIFTYP_ID)='I';

COMMIT;

