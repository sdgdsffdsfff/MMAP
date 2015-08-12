/*
目标表：产品信息表(ST_PRODUCT)
源表：	DMMKT层三张产品表：
	投资理财类产品信息表(PDInvFs)
	资产类产品信息表(PDAsset)
	负债类产品信息表(PDLiabi)
开发人:	郭盼盼
开日期:	20150731
特殊说明:
变更记录:
*/
TRUNCATE TABLE MMAPST.ST_PRODUCT;
INSERT INTO MMAPST.ST_PRODUCT
(
	ETL_DATE	--跑批日期
	,PROD_CODE	--产品代码
	,PROD_DESC	--产品描述
	,PROD_TYP_CODE	--产品类型代码
	,GLGrp_ID	--总账组号
	,RefYield_Rate	--参考收益率
	,CustYield_Rate	--客户收益率
	,YieldMod_ID	--收益模式
	,Term_No	--产品期限
	,TermUnit_ID	--期限单位
	,MinHld_Amt	--最少持有金额(人民币)
	,R_Nav		--产品净值
	,IncmBeg_Dt	--计息开始日期
	,IncmEnd_Dt	--计息结束日期
	,SourceGrp	--数据源组
)
SELECT
	to_number(to_char(SYSDATE,'YYYYMMDD')) AS ETL_DATE
	,UPPER(TRIM(ProdTyp_ID)) AS PROD_CODE
	,REPLACE(REPLACE(TRIM(ProdTyp_NM),'“',''),'”','') AS PROD_DESC
	,TRIM(ProdCat_ID) AS PROD_TYP_CODE
	,null	 AS GLGrp_ID
	,RefYield_Rate
	,CustYield_Rate
	,TRIM(YieldMod_ID) AS YieldMod_ID
	,TRIM(Term_No) AS Term_No
	,TRIM(TermUnit_ID) AS TermUnit_ID
	,MinHld_Amt
	,R_Nav
	,IncmBeg_Dt
	,IncmEnd_Dt
	,'PDINVFS' AS	SourceGrp
FROM        MMAPST.DMMKT_PDInvFs
WHERE TRIM(ProdTyp_NM)<>'(null)'
UNION ALL
SELECT
	to_number(to_char(SYSDATE,'YYYYMMDD'))
	,UPPER(TRIM(ProdTyp_ID))
	,REPLACE(REPLACE(TRIM(ProdTyp_NM),'“',''),'”','')
	,TRIM(ProdCat_ID)
	,TRIM(GLGrp_ID)
	,NULL
	,NULL
	,NULL
	,NULL
	,NULL
	,NULL
	,NULL
	,NULL
	,NULL
	,'PDASSET'
FROM MMAPST.DMMKT_PDAsset
WHERE TRIM(ProdTyp_NM)<>'(null)'
UNION ALL
SELECT
	to_number(to_char(SYSDATE,'YYYYMMDD'))
	,UPPER(TRIM(ProdTyp_ID))
	,REPLACE(REPLACE(TRIM(ProdTyp_NM),'“',''),'”','')
	,TRIM(ProdCat_ID)
	,TRIM(GLGrp_ID)
	,NULL
	,NULL
	,NULL
	,NULL
	,NULL
	,NULL
	,NULL
	,NULL
	,NULL
	,'PDLIABI'
FROM MMAPST.DMMKT_PDLiabi
WHERE TRIM(ProdTyp_NM)<>'(null)'
