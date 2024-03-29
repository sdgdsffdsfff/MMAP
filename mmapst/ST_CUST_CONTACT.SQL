TRUNCATE TABLE MMAPST.ST_CUST_CONTACT;
INSERT INTO MMAPST.ST_CUST_CONTACT 
(
	ETL_DATE
	,TX_DATE
	,PERIOD_ID
	,CUSTOMER_ID
	,CONTACT_PHONE_NO
	,CONTACT_PHONE_EXT
	,CONTACT_ADDR
	,CONTACT_POST_CODE
	,CONTACT_CITY_CODE
	,CONTACT_STATE_CODE
	,CONTACT_COUNTRY_CODE
	,REG_PHONE_NO
	,REG_PHONE_EXT
	,REG_ADDR
	,REG_POST_CODE
	,REG_CITY_CODE
	,REG_STATE_CODE
	,REG_COUNTRY_CODE
	,CMP_PHONE_NO
	,CMP_PHONE_EXT
	,CMP_ADDR
	,CMP_DEP_NAME
	,CMP_POST_CODE
	,CMP_CITY_CODE
	,CMP_STATE_CODE
	,CMP_COUNTRY_CODE
	,MOBILE_PHONE1_NO
	,MOBILE_PHONE2_NO
	,MOBILE_PHONE3_NO
	,FAX_NO
) 

SELECT
	TO_NUMBER(TO_CHAR(SYSDATE,'YYYYMMDD')) AS ETL_DATE  
	,a.TX_DT
	,TO_NUMBER(TO_CHAR(SYSDATE,'YYYYMMDD')) AS PERIOD_ID
	,TRIM(a.CIF_ORG)
	,CASE  a.ELECTYP_ID WHEN 'PH' THEN  Trim(a.ELECADDR) END AS CONTACT_PHONE_NO
	,NULL AS CONTACT_PHONE_EXT
	,CASE  b.ADDTYP_ID WHEN 'H' THEN  Trim(b.ADDR) END AS CONTACT_ADDR
	,NULL AS CONTACT_POST_CODE
	,NULL AS CONTACT_CITY_CODE
	,NULL AS CONTACT_STATE_CODE
	,NULL AS CONTACT_COUNTRY_CODE
	,NULL AS REG_PHONE_NO
	,NULL AS REG_PHONE_EXT
	,NULL AS REG_ADDR
	,NULL AS REG_POST_CODE
	,NULL AS REG_CITY_CODE
	,NULL AS REG_STATE_CODE
	,NULL AS REG_COUNTRY_CODE
	,CASE  a.ELECTYP_ID WHEN 'PO' THEN  Trim(a.ElecAddr) END AS CMP_PHONE_NO
	,NULL AS CMP_PHONE_EXT
	,CASE  b.ADDTYP_ID WHEN 'E' THEN  Trim(b.ADDR) END AS CONTACT_ADDR
	,NULL AS CMP_DEP_NAME
	,NULL AS CMP_POST_CODE
	,NULL AS CMP_CITY_CODE
	,NULL AS CMP_STATE_CODE
	,NULL AS CMP_COUNTRY_CODE
	,CASE  a.ELECTYP_ID WHEN 'PM' THEN  Trim(a.ElecAddr) END AS MOBILE_PHONE1_NO
	,NULL AS MOBILE_PHONE2_NO
	,NULL AS MOBILE_PHONE3_NO
	,CASE  a.ELECTYP_ID WHEN 'FX' THEN  Trim(a.ElecAddr) END AS FAX_NO
FROM MMAPST.DMMKT_CSCONN a,MMAPST.DMMKT_CSADDR b
WHERE a.CIF_ORG=b.CIF_ORG


