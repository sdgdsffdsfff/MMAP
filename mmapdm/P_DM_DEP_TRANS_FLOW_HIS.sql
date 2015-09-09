CREATE OR REPLACE PROCEDURE P_DM_DEP_TRANS_FLOW_HIS
(IO_STATUS OUT INTEGER,IO_SQLERR OUT VARCHAR2)
AS
	TABLE_NAME VARCHAR2(125) :='DM_DEP_TRANS_FLOW_HIS'; -- ����
	PROCEDURE_NAME VARCHAR2(125) :='P_'||TABLE_NAME;  -- �洢������(�޸�)
	
	DM_SQL VARCHAR2(20000); -- ���SQL���
	
	IO_ROW INTEGER;  --��������
	ST_ROW INTEGER;  --Դ��������
	
	V_ETL_DATE NUMBER;  -- ��������
	V_START_TIMESTAMP TIMESTAMP;    -- ���ؿ�ʼʱ��
	V_END_TIMESTAMP   TIMESTAMP;    -- ���ؽ���ʱ��
	
	DM_TODAY NUMBER;        -- ��������"����"
	TX_DATE NUMBER;

BEGIN
	SELECT SYSDATE INTO V_START_TIMESTAMP FROM dual;    -- ���س������п�ʼʱ��
	
	SELECT TO_NUMBER(TO_CHAR((SYSDATE),'YYYYMMDD')) INTO V_ETL_DATE FROM dual;  -- ȡϵͳ������Ϊ��������
	SELECT TX_DATE INTO DM_TODAY FROM MMAPST.ST_SYSTEM_DATE;  -- ȡ��������
	
	--��ѯST������Ƿ���'����'����
  SELECT COUNT(1) INTO ST_ROW FROM MMAPST.ST_DEP_TRANS_FLOW WHERE PERIOD_ID=DM_TODAY;
  
  --���ST����"����"���ݣ���������ݳ�ȡ�����򱣳�DM���������ݲ��䡣
  IF ST_ROW>0
  THEN
    /*���ͬһ���ظ�ִ�д˴洢���̵��쳣���������ǰ��ɾ��"����"����*/
    DELETE FROM MMAPDM.DM_DEP_TRANS_FLOW_HIS WHERE PERIOD_ID = DM_TODAY;    -- ɾ��"����"����
    COMMIT;
    
    /*���뵱�ս�������*/
    DM_SQL := 'INSERT INTO DM_DEP_TRANS_FLOW_HIS
      (
        ETL_DATE
        ,TX_DATE
        ,PERIOD_ID
        ,TRANS_DT
        ,TRANS_TM
        ,SER_NO
        ,ISER_NO
        ,OPER_ORG
        ,CUSTOMER_ID
        ,ACCOUNT_ID
        ,ACCTTYP_ID
        ,BUSSTYP_ID
        ,TRANSCDE_ID
        ,TREATMENT_CODE
        ,TRANSDIR_FG
        ,TRANSST_ID
        ,TRANSBR_ID
        ,CHN_ID
        ,PRODCDE_ID
        ,PRODTYP_ID
        ,TRANS_AMT1
        ,AFTRANS_AMT1
        ,TRANSCURR_ID
        ,TRANS_AMT
        ,FEECURR_ID
        ,TRANS_FEE
        ,OACCTCURR_ID
        ,REMARK
        ,PURPDESC
        ,IMGIDX_NO
        ,IMGTYP_ID
        ,EFFECT_DT
        ,EFFECT_TM
        ,CHCK_NO
        ,TRANSACCTTYP_ID
        ,TRANSACCTCURR_ID
        ,TRANS_BAL
        ,TRANSACCT_ORG
        ,CARD_ORG
        ,SCNTRY_ID
        ,SENTITY_ID
        ,MERC_ID
        ,REVCARD_NM
        ,MERCTYP_ID
        ,MERC_NM
        ,SOURCEGRP
      )
      SELECT
        '|| V_ETL_DATE ||'
        ,A.TX_DATE
        ,A.PERIOD_ID
        ,TRANS_DT
        ,TRANS_TM
        ,SER_NO
        ,ISER_NO
        ,OPER_ORG
        ,CUSTOMER_ID
        ,ACCOUNT_ID
        ,ACCTTYP_ID
        ,BUSSTYP_ID
        ,TRANSCDE_ID
        ,TREATMENT_CODE
        ,TRANSDIR_FG
        ,TRANSST_ID
        ,TRANSBR_ID
        ,CHN_ID
        ,PRODCDE_ID
        ,B.PROD_TYPE_CODE
        ,TRANS_AMT1
        ,AFTRANS_AMT1
        ,TRANSCURR_ID
        ,TRANS_AMT
        ,FEECURR_ID
        ,TRANS_FEE
        ,OACCTCURR_ID
        ,REMARK
        ,PURPDESC
        ,IMGIDX_NO
        ,IMGTYP_ID
        ,EFFECT_DT
        ,EFFECT_TM
        ,CHCK_NO
        ,TRANSACCTTYP_ID
        ,TRANSACCTCURR_ID
        ,TRANS_BAL
        ,TRANSACCT_ORG
        ,CARD_ORG
        ,SCNTRY_ID
        ,SENTITY_ID
        ,MERC_ID
        ,REVCARD_NM
        ,MERCTYP_ID
        ,MERC_NM
        ,SOURCEGRP
      FROM  MMAPST.ST_DEP_TRANS_FLOW A
      LEFT  JOIN MMAPDM.DM_PRODUCT B
      ON  A.PRODCDE_ID=B.PROD_CODE';
    EXECUTE IMMEDIATE DM_SQL;
    IO_ROW := SQL%ROWCOUNT ;
    COMMIT;
  END IF;
  
  
  /*д����־��Ϣ*/    
  SELECT SYSDATE INTO V_END_TIMESTAMP   FROM dual;    -- ���س������н���ʱ��
  IO_STATUS := 0 ;
  IO_SQLERR := 'SUSSCESS';
  P_MMAPDM_WRITE_LOGS(PROCEDURE_NAME,IO_STATUS,IO_ROW,V_START_TIMESTAMP,V_END_TIMESTAMP,IO_SQLERR);
  COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
  ROLLBACK ;
  IO_STATUS := 9 ;
  IO_SQLERR := SQLCODE || SQLERRM ;
  SELECT SYSDATE INTO V_END_TIMESTAMP FROM dual;
  P_MMAPDM_WRITE_LOGS(PROCEDURE_NAME,IO_STATUS,IO_ROW,V_START_TIMESTAMP,V_END_TIMESTAMP,IO_SQLERR);
END P_DM_DEP_TRANS_FLOW_HIS;
/
