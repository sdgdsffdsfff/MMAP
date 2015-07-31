INSERT INTO MMAPST.ST_PRODUCT
(
	ETL_DATE	--��������
	,PROD_CODE	--��Ʒ����
	,PROD_DESC	--��Ʒ����
	,PROD_TYP_CODE	--��Ʒ���ʹ���
	,GLGrp_ID	--�������
	,RefYield_Rate	--�ο�������
	,CustYield_Rate	--�ͻ�������
	,YieldMod_ID	--����ģʽ
	,Term_No	--��Ʒ����
	,TermUnit_ID	--���޵�λ
	,MinHld_Amt	--���ٳ��н��(�����)
	,R_Nav		--��Ʒ��ֵ
	,IncmBeg_Dt	--��Ϣ��ʼ����
  ,IncmEnd_Dt  --��Ϣ��������
  ,SourceGrp  --����Դ��
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

