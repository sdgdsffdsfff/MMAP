DROP TABLE  MMAPST.ST_CUST_AGREEMENT;

CREATE TABLE
    MMAPST.ST_CUST_AGREEMENT
    (
         ETL_DATE number(8)
        ,TX_DATE date
        ,PERIOD_ID number(8)
        ,CUSTOMER_ID varchar2(50)
        ,ACCOUNT_ID varchar2(50)
        ,AGMT_TYPE_CODE varchar2(50)
        ,AGMT_CURR varchar2(50)
        ,AGMT_AMT varchar2(50)
        ,AGMT_STATUS_CODE varchar2(50)
        ,AGMT_BRANCH varchar2(50)
        ,AGMT_CHANEL_CODE varchar2(50)
        ,AGMT_SIGN_DT date
        ,AGMT_START_DT date
        ,AGMT_END_DT date
        ,AGMT_REMARK varchar2(50)
        ,SOURCEGRP varchar2(50)
    ) ;

INSERT INTO  MMAPST.ST_CUST_AGREEMENT
(
         ETL_DATE
        ,TX_DATE
        ,PERIOD_ID
        ,CUSTOMER_ID
        ,ACCOUNT_ID
        ,AGMT_TYPE_CODE
        ,AGMT_CURR
        ,AGMT_AMT
        ,AGMT_STATUS_CODE
        ,AGMT_BRANCH
        ,AGMT_CHANEL_CODE
        ,AGMT_SIGN_DT
        ,AGMT_START_DT
        ,AGMT_END_DT
        ,AGMT_REMARK
        ,SOURCEGRP
)
SELECT
        TO_NUMBER(TO_CHAR(SYSDATE,'YYYYMMDD')) AS ETL_DATE
        ,a.TX_DT AS TX_DATE
        ,TO_NUMBER(to_char(a.TX_DT,'YYYYMMDD')) AS PERIOD_ID
        ,a.CIF_ORG
        ,b.ACC_ORG
        ,'NB' AS AGMT_TYPE_CODE  --网银
        ,CURR_ID
        ,NULL
        ,Over_FG
        ,BRNH_ID
        ,CHN_ID
        ,SIGN_DT
        ,START_DT
        ,END_DT
        ,REMARKS
        ,'AGElecSvc' AS SOURCEGRP  
FROM MMAPST.DMMKT_CSMAST a 
INNER JOIN
(
     SELECT * FROM MMAPST.DMMKT_AGElecSvc 
     WHERE prodtyp_id='0' AND over_fg='0'AND SOURCE_ID='WY' 
) b
ON TRIM(a.cif_org)=TRIM(b.cif_org)
WHERE TRIM(a.CIFTYP_ID)='I'
AND a.TX_DT = ( SELECT TO_DATE(TX_DATE,'YYYYMMDD') FROM MMAPST.DMMKT_SYSTEM_DATE)
UNION 
SELECT
         TO_NUMBER(TO_CHAR(SYSDATE,'YYYYMMDD')) AS ETL_DATE
         ,a.TX_DT AS TX_DATE
         ,TO_NUMBER(to_char(a.TX_DT,'YYYYMMDD')) AS PERIOD_ID
         ,a.CIF_ORG
         ,b.ACC_ORG
         ,'MB' AS AGMT_TYPE_CODE  --手机银行
         ,CURR_ID
         ,NULL
         ,Over_FG
         ,BRNH_ID
         ,CHN_ID
         ,SIGN_DT
         ,START_DT
         ,END_DT
         ,REMARKS
         ,'AGElecSvc' AS SOURCEGRP  
FROM MMAPST.DMMKT_CSMAST a 
INNER JOIN
(
   SELECT * FROM MMAPST.DMMKT_AGElecSvc 
   WHERE prodtyp_id='1' AND over_fg='0'
) b
ON TRIM(a.cif_org)=trim(b.cif_org)
WHERE TRIM(a.CIFTYP_ID)='I'
AND a.TX_DT = ( SELECT TO_DATE(TX_DATE,'YYYYMMDD') FROM MMAPST.DMMKT_SYSTEM_DATE)
UNION 
SELECT  
          TO_NUMBER(TO_CHAR(SYSDATE,'YYYYMMDD')) AS ETL_DATE
          ,TX_DT AS TX_DATE
          ,TO_NUMBER(to_char(a.TX_DT,'YYYYMMDD')) AS PERIOD_ID
          ,a.CIF_ORG AS CUSTOMER_ID
          ,b.ACC_ORG
          ,'ENTRUST' AS AGMT_TYPE_CODE  --委托代扣
          ,CURR_ID
          ,NULL
          ,Over_FG
          ,BRNH_ID
          ,CHN_ID
          ,SIGN_DT
          ,START_DT
          ,END_DT
          ,REMARKS
          ,'AGEntrsRP' AS SOURCEGRP
FROM MMAPST.DMMKT_CSMast a 
INNER JOIN
(
SELECT DISTINCT TRIM(a.CIF_ORG) AS CIF_ORG
          ,a.ACC_ORG
          ,b.SIGN_DT 
          ,b.START_DT
          ,b.END_DT
          ,TRIM(b.BRNH_ID) AS BRNH_ID
          ,TRIM(b.OVER_FG) AS OVER_FG
          ,TRIM(b.REMARKS) AS REMARKS
          ,TRIM(b.PRODTYP_ID) AS PRODTYP_ID
          ,TRIM(b.CURR_ID) AS CURR_ID
          ,TRIM(b.SOURCE_ID) AS SOURCE_ID
          ,TRIM(b.CHN_ID) AS CHN_ID
          ,TRIM(b.BIZ_ID) AS BIZ_ID
FROM  MMAPST.DMMKT_CSACCREL a
INNER JOIN
MMAPST.DMMKT_AGEntrsRP  b
ON TRIM(a.ACC_ORG)=TRIM(b.ACC_ORG)
WHERE b.SOURCE_ID='DK'
)b
ON TRIM(a.cif_org)=TRIM(b.cif_org)
WHERE a.CIFTYP_ID='I'
AND a.TX_DT = ( SELECT TO_DATE(TX_DATE,'YYYYMMDD') FROM MMAPST.DMMKT_SYSTEM_DATE)
UNION
SELECT  
          TO_NUMBER(TO_CHAR(SYSDATE,'YYYYMMDD')) AS ETL_DATE
          ,a.TX_DT AS TX_DATE
          ,TO_NUMBER(to_char(a.TX_DT,'YYYYMMDD')) AS PERIOD_ID
          ,a.CIF_ORG AS CUSTOMER_ID
          ,b.ACC_ORG
          ,'WATER' AS AGMT_TYPE_CODE  --代扣水煤气
          ,b.CURR_ID
          ,NULL
          ,b.OVER_FG
          ,b.BRNH_ID
          ,b.CHN_ID
          ,b.SIGN_DT
          ,b.START_DT
          ,b.END_DT
          ,b.REMARKS
          ,'AGEntrsBP' AS SOURCEGRP
FROM MMAPST.DMMKT_CSMast a 
INNER JOIN
(
SELECT DISTINCT TRIM(a.CIF_ORG) AS CIF_ORG
           ,a.ACC_ORG
           ,b.SIGN_DT 
           ,b.START_DT
           ,b.END_DT
           ,TRIM(b.BRNH_ID) AS BRNH_ID
           ,TRIM(b.REMARKS) AS REMARKS
           ,TRIM(b.PRODTYP_ID) AS PRODTYP_ID
           ,TRIM(b.CURR_ID) AS CURR_ID
           ,TRIM(b.OVER_FG) AS OVER_FG
           ,TRIM(b.SOURCE_ID) AS SOURCE_ID
           ,TRIM(b.CHN_ID) AS CHN_ID
           ,TRIM(b.BIZ_ID) AS BIZ_ID
FROM  MMAPST.DMMKT_CSACCREL a
INNER JOIN
(
     SELECT m.* FROM MMAPST.DMMKT_AGEntrsRP m
     INNER JOIN MMAPST.DMMKT_AGEntrsBP n
     ON m.BIZ_ID=n.BIZ_ID
     WHERE m.SOURCE_ID='DK' AND n.PRODTYP_ID='PUB'
)  b
ON TRIM(a.ACC_ORG)=TRIM(b.ACC_ORG)
)b
ON TRIM(a.cif_org)=TRIM(b.cif_org)
WHERE TRIM(a.CIFTYP_ID)='I'
AND a.TX_DT = ( SELECT TO_DATE(TX_DATE,'YYYYMMDD') FROM MMAPST.DMMKT_SYSTEM_DATE)
UNION
SELECT  
          TO_NUMBER(TO_CHAR(SYSDATE,'YYYYMMDD')) AS ETL_DATE
          ,TX_DT AS TX_DATE
          ,TO_NUMBER(to_char(a.TX_DT,'YYYYMMDD')) AS PERIOD_ID
          ,a.CIF_ORG AS CUSTOMER_ID
          ,b.ACC_ORG
          ,'POWER' AS AGMT_TYPE_CODE  --代扣电费
          ,CURR_ID
          ,'AGMT_AMT'
          ,OVER_FG
          ,BRNH_ID
          ,CHN_ID
          ,SIGN_DT
          ,START_DT
          ,END_DT
          ,REMARKS
          ,'AGEntrsBP' AS SOURCEGRP
FROM MMAPST.DMMKT_CSMast a 
INNER JOIN
(
          SELECT DISTINCT TRIM(a.CIF_ORG) AS CIF_ORG
          ,a.ACC_ORG
          ,b.SIGN_DT 
          ,b.START_DT
          ,b.END_DT
          ,TRIM(b.BRNH_ID) AS BRNH_ID
          ,TRIM(b.OVER_FG) AS OVER_FG
          ,TRIM(b.REMARKS) AS REMARKS
          ,TRIM(b.PRODTYP_ID) AS PRODTYP_ID
          ,TRIM(b.CURR_ID) AS CURR_ID
          ,TRIM(b.SOURCE_ID) AS SOURCE_ID
          ,TRIM(b.CHN_ID) AS CHN_ID
          ,TRIM(b.BIZ_ID) AS BIZ_ID
FROM  MMAPST.DMMKT_CSACCREL a
INNER JOIN
(
      SELECT m.* FROM MMAPST.DMMKT_AGEntrsRP m
      INNER JOIN MMAPST.DMMKT_AGEntrsBP n
      ON m.BIZ_ID=n.BIZ_ID
      WHERE m.SOURCE_ID='DK' AND n.PRODTYP_ID='POW'
)  b
ON TRIM(a.ACC_ORG)=TRIM(b.ACC_ORG)
)b
ON TRIM(a.cif_org)=TRIM(b.cif_org)
WHERE a.CIFTYP_ID='I'
AND a.TX_DT = ( SELECT TO_DATE(TX_DATE,'YYYYMMDD') FROM MMAPST.DMMKT_SYSTEM_DATE)
UNION
SELECT  
          TO_NUMBER(TO_CHAR(SYSDATE,'YYYYMMDD')) AS ETL_DATE
          ,TX_DT AS TX_DATE
          ,TO_NUMBER(to_char(a.TX_DT,'YYYYMMDD')) AS PERIOD_ID
          ,a.CIF_ORG AS CUSTOMER_ID
          ,b.ACC_ORG
          ,'TELE_COMM_IND' AS AGMT_TYPE_CODE  --代扣通讯费
          ,CURR_ID
          ,NULL
          ,OVER_FG
          ,BRNH_ID
          ,CHN_ID
          ,SIGN_DT
          ,START_DT
          ,END_DT
          ,REMARKS
          ,'AGEntrsBP' AS SOURCEGRP
FROM MMAPST.DMMKT_CSMast a 
INNER JOIN
(
SELECT DISTINCT TRIM(a.CIF_ORG) AS CIF_ORG
          ,a.ACC_ORG
          ,b.SIGN_DT 
          ,b.START_DT
          ,b.END_DT
          ,TRIM(b.BRNH_ID) AS BRNH_ID
          ,TRIM(b.OVER_FG) AS OVER_FG
          ,TRIM(b.REMARKS) AS REMARKS
          ,TRIM(b.PRODTYP_ID) AS PRODTYP_ID
          ,TRIM(b.CURR_ID) AS CURR_ID
          ,TRIM(b.SOURCE_ID) AS SOURCE_ID
          ,TRIM(b.CHN_ID) AS CHN_ID
          ,TRIM(b.BIZ_ID) AS BIZ_ID
FROM  MMAPST.DMMKT_CSACCREL a
INNER JOIN
(
     SELECT m.* FROM MMAPST.DMMKT_AGEntrsRP m
     INNER JOIN MMAPST.DMMKT_AGEntrsBP n
     ON m.BIZ_ID=n.BIZ_ID
     WHERE m.SOURCE_ID='DK' AND n.PRODTYP_ID IN ('CUC','DXF')
)  b
ON TRIM(a.ACC_ORG)=TRIM(b.ACC_ORG)
)b
ON TRIM(a.cif_org)=TRIM(b.cif_org)
WHERE a.CIFTYP_ID='I'
AND a.TX_DT = ( SELECT TO_DATE(TX_DATE,'YYYYMMDD') FROM MMAPST.DMMKT_SYSTEM_DATE)
UNION
SELECT  
         TO_NUMBER(TO_CHAR(SYSDATE,'YYYYMMDD')) AS ETL_DATE
         ,a.TX_DT AS TX_DATE
         ,TO_NUMBER(to_char(a.TX_DT,'YYYYMMDD')) AS PERIOD_ID
         ,a.CIF_ORG AS CUSTOMER_ID
         ,b.ACC_ORG
         ,'PAYROLL' AS AGMT_TYPE_CODE  --代发工资户
         ,CURR_ID
         ,NULL
         ,OVER_FG
         ,b.BRNH_ID
         ,CHN_ID
         ,b.SIGN_DT
         ,b.START_DT
         ,b.END_DT
         ,REMARKS
         ,'AGEmployRP' AS SOURCEGRP
FROM MMAPST.DMMKT_CSMast a
INNER JOIN
(
   SELECT DISTINCT a.CIF_ORG
         ,a.ACC_ORG
         ,b.SIGN_DT 
         ,b.START_DT
         ,b.END_DT
         ,TRIM(b.BRNH_ID) AS BRNH_ID
         ,TRIM(b.OVER_FG) AS OVER_FG
         ,TRIM(b.REMARKS) AS REMARKS
         ,TRIM(b.PRODTYP_ID) AS PRODTYP_ID
         ,TRIM(b.CURR_ID) AS CURR_ID
         ,TRIM(b.SOURCE_ID) AS SOURCE_ID
         ,TRIM(b.CHN_ID) AS CHN_ID
         ,TRIM(b.BIZ_ID) AS BIZ_ID
 FROM MMAPST.DMMKT_CSAccRel a 
 INNER JOIN
    (SELECT  a.ACC_ORG
         ,b.SIGN_DT 
         ,b.START_DT
         ,b.END_DT
         ,TRIM(b.BRNH_ID) AS BRNH_ID
         ,TRIM(b.OVER_FG) AS OVER_FG
         ,TRIM(b.REMARKS) AS REMARKS
         ,TRIM(b.PRODTYP_ID) AS PRODTYP_ID
         ,TRIM(b.CURR_ID) AS CURR_ID
         ,TRIM(b.SOURCE_ID) AS SOURCE_ID
         ,TRIM(b.CHN_ID) AS CHN_ID
         ,TRIM(b.BIZ_ID) AS BIZ_ID
     FROM  MMAPST.DMMKT_AGEmployRP a
     INNER JOIN  MMAPST.DMMKT_AGEntrsRP b
     ON TRIM(a.CIF_ORG)=TRIM(b.CIF_ORG)
     WHERE a.PAY_FG='1'AND b.SOURCE_ID='DF'
    ) b
ON TRIM(a.Acc_org)=TRIM(b.acc_org)
)b
ON TRIM(a.CIF_ORG)=TRIM(b.CIF_ORG)
WHERE TRIM(a.CIFTYP_ID)='I'
AND a.TX_DT = ( SELECT TO_DATE(TX_DATE,'YYYYMMDD') FROM MMAPST.DMMKT_SYSTEM_DATE)
UNION
SELECT
         TO_NUMBER(TO_CHAR(SYSDATE,'YYYYMMDD')) AS ETL_DATE
         ,b.TX_DT AS TX_DATE
         ,TO_NUMBER(to_char(a.TX_DT,'YYYYMMDD')) AS PERIOD_ID
         ,b.CIF_ORG AS CUSTOMER_ID
         ,b.ACC_ORG
         ,'SMS' AS AGMT_TYPE_CODE  --开通短信
         ,NULL
         ,NULL
         ,OVER_FG
         ,b.BRNH_ID
         ,NULL
         ,b.START_DT
         ,b.START_DT
         ,NULL
         ,NULL
         ,'AGMsgCif' AS SOURCEGRP
FROM MMAPST.DMMKT_CSMast a
INNER JOIN
(
SELECT TX_DT
        ,CIF_ORG
        ,ACC_ORG
        ,BRNH_ID
        ,START_DT
        ,OVER_FG 
FROM MMAPST.DMMKT_AGMsgCif 
WHERE over_fg='1' AND SOURCE_ID='DX'
) b
ON TRIM(a.cif_org)=TRIM(b.cif_org)
WHERE TRIM(a.CIFTYP_ID)='I'
AND a.TX_DT = ( SELECT TO_DATE(TX_DATE,'YYYYMMDD') FROM MMAPST.DMMKT_SYSTEM_DATE)
UNION
SELECT
         TO_NUMBER(TO_CHAR(SYSDATE,'YYYYMMDD')) AS ETL_DATE
        ,b.TX_DT AS TX_DATE
        ,TO_NUMBER(to_char(a.TX_DT,'YYYYMMDD')) AS PERIOD_ID
        ,b.CIF_ORG AS CUSTOMER_ID
        ,b.ACC_ORG
        ,'BP' AS AGMT_TYPE_CODE			--青银直通车
        ,CURR_ID
        ,NULL
        ,Over_FG
        ,b.BRNH_ID
        ,CHN_ID
        ,b.SIGN_DT
        ,b.START_DT
        ,END_DT
        ,REMARKS
        ,'AGpmt' AS SOURCEGRP
FROM MMAPST.DMMKT_CSMast a  
INNER JOIN
(
SELECT  TX_DT
        ,CIF_ORG
        ,ACC_ORG
        ,BRNH_ID
        ,CHN_ID
        ,SIGN_DT
        ,START_DT
        ,END_DT
        ,REMARKS
        ,CURR_ID
        ,Over_FG FROM MMAPST.DMMKT_AGpmt WHERE SOURCE_ID='BP'
) b
ON TRIM(a.cif_org)=TRIM(b.cif_org)
WHERE TRIM(a.CIFTYP_ID)='I'
AND a.TX_DT = ( SELECT TO_DATE(TX_DATE,'YYYYMMDD') FROM MMAPST.DMMKT_SYSTEM_DATE)
;
COMMIT;