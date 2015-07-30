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
	,CUST_LC_CD_BAL             --活期存款余额（人民币）
	,CUST_LC_TD_BAL             --定期存款余额（人民币）
	,CUST_LC_LOAN_BAL           --贷款余额（人民币）
	,CUST_LC_NDEBT_BAL          --国债余额（人民币）
	,CUST_LC_FOUND_BAL          --基金余额（人民币）
	,CUST_LC_FIN_BAL            --理财余额（人民币）
	,CUST_LC_INSURE_BAL         --保险余额（人民币）
	,CUST_LC_NOBLE_BAL          --贵金属余额（人民币）
	,CUST_LC_CREDIT_BAL         --信用卡消费额（人民币）
	,CUST_LC_ALL_BAL            --客户总资产余额（人民币）
	,CUST_FC_CD_BAL             --活期存款余额（外币折合人民币）
	,CUST_FC_TD_BAL             --定期存款余额（外币折合人民币）
	,CUST_FC_LOAN_BAL           --贷款余额（外币折合人民币）
	,CUST_FC_CREDIT_BAL         --信用卡消费额（外币折合人民币）
	,CUST_FC_ALL_BAL            --客户总资产余额（外币折合人民币）
	,CUST_ALL_BAL               --客户总资产余额（人民币+外币）
)

SELECT 
TO_NUMBER (TO_CHAR(SYSDATE,'yyyymmdd')) AS ETL_DATE
	,Tx_DT
	,TO_CHAR (Tx_DT,'yyyymmdd') AS period_ID
	,TRIM (CIF_ORG) AS CUSTOMER_ID
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
FROM MMAPST.DMMKT_CSLvlCBal;
