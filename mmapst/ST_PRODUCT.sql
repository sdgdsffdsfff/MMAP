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
  ,IncmEnd_Dt  --计息结束日期
  ,SourceGrp  --数据源组
)
SELECT
  to_number(to_char(SYSDATE,'YYYYMMDD')) AS ETL_DATE
  ,TRIM(ProdTyp_ID) AS PROD_CODE
  ,TRIM(ProdTyp_NM) AS PROD_DESC
  ,TRIM(ProdCat_ID) AS PROD_TYP_CODE
  ,null  AS GLGrp_ID
  ,RefYield_Rate
  ,CustYield_Rate
  ,TRIM(YieldMod_ID) AS  YieldMod_ID
  ,TRIM(Term_No) AS Term_No
  ,TRIM(TermUnit_ID) AS  TermUnit_ID
  ,MinHld_Amt
  ,R_Nav
  ,IncmBeg_Dt
  ,IncmEnd_Dt
  ,'PDInvFs' AS SourceGrp
FROM  MMAPST.DMMKT_PDInvFs
UNION ALL
SELECT
  to_number(to_char(SYSDATE,'YYYYMMDD'))
  ,TRIM(ProdTyp_ID)
  ,TRIM(ProdTyp_NM)
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
  ,'PDAsset'
FROM MMAPST.DMMKT_PDAsset
UNION ALL
SELECT
  to_number(to_char(SYSDATE,'YYYYMMDD'))
  ,TRIM(ProdTyp_ID)
  ,TRIM(ProdTyp_NM)
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
  ,'PDLiabi'
FROM MMAPST.DMMKT_PDLiabi

