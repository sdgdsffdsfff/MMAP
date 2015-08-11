/*
	客户余额表 ST_CUST_BAL
	author:XM
	date:2015-07-30
*/

INSERT INTO ST_CUST_BAL
(
   ETL_DATE                   --跑批日期
  ,TX_DATE                    --数据日期
  ,period_ID                  --日期
  ,CUSTOMER_ID                --客户号
  ,CUST_CD_BAL_LC             --活期存款余额（人民币）
  ,CUST_TD_BAL_LC             --定期存款余额（人民币）
  ,CUST_LOAN_BAL_LC           --贷款余额（人民币）
  ,CUST_NDEBT_BAL_LC          --国债余额（人民币）
  ,CUST_FOUND_BAL_LC          --基金余额（人民币）
  ,CUST_FIN_BAL_LC            --理财余额（人民币）
  ,CUST_INSURE_BAL_LC         --保险余额（人民币）
  ,CUST_NOBLE_BAL_LC          --贵金属余额（人民币）
  ,CUST_CREDIT_BAL_LC         --信用卡消费额（人民币）
  ,CUST_ALL_BAL_LC            --客户总资产余额（人民币）
  ,CUST_CD_BAL_FC             --活期存款余额（外币折合人民币）
  ,CUST_TD_BAL_FC             --定期存款余额（外币折合人民币）
  ,CUST_LOAN_BAL_FC           --贷款余额（外币折合人民币）
  ,CUST_CREDIT_BAL_FC         --信用卡消费额（外币折合人民币）
  ,CUST_ALL_BAL_FC            --客户总资产余额（外币折合人民币）
  ,CUST_ALL_BAL               --客户总资产余额（人民币+外币）
)

SELECT 
  TO_NUMBER (TO_CHAR(SYSDATE,'yyyymmdd')) AS ETL_DATE
  ,a.Tx_DT
  ,TO_NUMBER (TO_CHAR (a.Tx_DT,'yyyymmdd')) AS period_ID
  ,TRIM (a.CIF_ORG) AS CUSTOMER_ID
  ,SD_LC_CBAL
  ,CD_LC_CBAL
  ,LN_LC_CBAL
  ,ETR_CBAL
  ,FS_CBAL
  ,LC_CBAL
  ,NULL AS CUST_LC_INSURE_BAL
  ,NULL AS CUST_LC_NOBLE_BAL
  ,NULL AS CUST_LC_CREDIT_BAL
  ,SD_LC_CBAL+CD_LC_CBAL+LN_LC_CBAL+ETR_CBAL+FS_CBAL+LC_CBAL AS CUST_LC_ALL_BAL
  ,SD_FC_CBAL
  ,CD_FC_CBAL
  ,LN_FC_CBAL
  ,NULL AS CUST_FC_CREDIT_BAL
  ,SD_FC_CBAL+CD_FC_CBAL+LN_FC_CBAL AS CUST_FC_ALL_BAL
  ,AS_CBAL
FROM DMMKT_CSLvlCBal a
inner join DMMKT_CSMast b on a.CIF_ORG=b.CIF_ORG
where trim(b.CIFTYP_ID)='I'


