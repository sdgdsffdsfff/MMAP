INSERT INTO MMAPST.ST_ITEM
select
   TO_NUMBER(TO_CHAR(SYSDATE,'YYYYMMDD')) AS ETL_DATE
  ,C.TX_DT AS TX_DATE
  ,TO_NUMBER(TO_CHAR(C.TX_DT,'YYYYMMDD')) AS PERIOD_ID
  ,ITEM_ID
  ,ITEM_TYP
  ,ITEM_NM
	,ITEM_FG
	,ITEM_DES
	,CODE_ID
	,BAK1
	,BAK2
	,BAK3
	,BAK4
	,BAK5
FROM mmapst.dmmkt_pcodemast a 
LEFT JOIN MMAPST.DMMKT_V_ZSYSTEM C
ON 1 = 1 