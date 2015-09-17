CREATE OR REPLACE PROCEDURE "MMAPDM"."P_DM_CUST_RELATION"
(IO_STATUS OUT INTEGER,IO_SQLERR OUT VARCHAR2)
AS

	TABLE_NAME VARCHAR2(125) :='CUST_RELATION';  -- 表名(修改)
BEGIN

--编辑取数SQL
DELETE FROM MMAPDM.DM_CUST_RELATION;
COMMIT;
INSERT INTO MMAPDM.DM_CUST_RELATION
(
	 ETL_DATE
	,TX_DATE
	,PERIOD_ID
	,CIF_ORG
	,RELCIF_ORG
	,RELTYP_CODE
	,RELTYP
	)
SELECT
	 ETL_DATE
	,TX_DATE
	,PERIOD_ID
	,CIF_ORG
	,RELCIF_ORG
	,RELTYP_CODE
	,RELTYP
	FROM MMAPST.ST_CUST_RELATION
COMMIT;

    P_DM_FULL(TABLE_NAME,500000,IO_STATUS,IO_SQLERR);
END P_DM_CUST_RELATION;
