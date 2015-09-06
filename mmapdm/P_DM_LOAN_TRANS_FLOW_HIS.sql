CREATE OR REPLACE PROCEDURE P_DM_LOAN_TRANS_FLOW_HIS
(IO_STATUS OUT INTEGER,IO_SQLERR OUT VARCHAR2)
AS
  TABLE_NAME VARCHAR2(125) :='DM_LOAN_TRANS_FLOW_HIS'; -- ����
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
  SELECT COUNT(1) INTO ST_ROW FROM MMAPST.ST_LOAN_TRANS_FLOW WHERE PERIOD_ID=DM_TODAY;

  --���ST����"����"���ݣ���������ݳ�ȡ�����򱣳�DM���������ݲ��䡣
  IF ST_ROW>0
  THEN
    /*���ͬһ���ظ�ִ�д˴洢���̵��쳣���������ǰ��ɾ��"����"����*/
    DELETE FROM MMAPDM.DM_LOAN_TRANS_FLOW_HIS WHERE PERIOD_ID = DM_TODAY;    -- ɾ��"����"����
    COMMIT;

    /*���뵱�ս�������*/
    DM_SQL := 'INSERT INTO DM_LOAN_TRANS_FLOW_HIS
      (
        ETL_DATE
        ,TX_DATE
        ,PERIOD_ID
        ,TRANS_DT
        ,TRANS_TM
        ,SER_NO
        ,OPER_ORG
        ,CUSTOMER_ID
        ,ACCOUNT_ID
        ,TRANSCDE_ID
        ,TREATMENT_CODE
        ,TRANSST_ID
        ,TRANSBR_ID
        ,CHN_ID
        ,PRODCDE_ID
        ,PRODTYP_ID
        ,BUSSTYP_ID
        ,TRANSCURR_ID
        ,TRANS_AMT
        ,FEECURR_ID
        ,TRANS_FEE
        ,OACCTCURR_ID
        ,REMARK
        ,PURPDESC
        ,PUTOUT_DT
        ,EXPIRE_DT
        ,BASE_INT
        ,DEFAULT_INT
        ,PAYMTHD_ID
        ,VOUCHTYPE_ID
        ,DEPOSITCURR_ID
        ,R_DEPOSITCURR
        ,EXPOSURE_AMT
        ,PAYHOLD_ID
        ,GUARTYPE_ID
        ,RESEACCT_ORG
        ,LOANACCT_ORG
        ,CLIENTACCT_ORG
        ,DEPOSITACCT_ORG
        ,FEEACC_ORG
        ,REVBRANCH_ID
        ,REVBRANCH_NM
        ,DEALSTART_TM
        ,DEALEND_TM
        ,DEAL_FG
        ,REGBRANCH_ID
        ,REVCIF_NM
        ,LOANTYPE_ID
        ,PROMISSORY_NO
        ,MNCONTRACT_NO
        ,CRAGREE_NO
        ,CONTRACT_AMT
        ,GUARN_NO
        ,BILL_AMT
        ,SUBJECT_NO
        ,SUBJECT2_NO
        ,SOURCEGRP
      )
      SELECT
        '|| V_ETL_DATE ||'
        ,A.TX_DATE
        ,A.PERIOD_ID
        ,TRANS_DT
        ,TRANS_TM
        ,SER_NO
        ,OPER_ORG
        ,CUSTOMER_ID
        ,ACCOUNT_ID
        ,TRANSCDE_ID
        ,TREATMENT_CODE
        ,TRANSST_ID
        ,TRANSBR_ID
        ,CHN_ID
        ,LoanType_ID
        ,B.PROD_TYPE_CODE
        ,BUSSTYP_ID
        ,TRANSCURR_ID
        ,TRANS_AMT
        ,FEECURR_ID
        ,TRANS_FEE
        ,OACCTCURR_ID
        ,REMARK
        ,PURPDESC
        ,PUTOUT_DT
        ,EXPIRE_DT
        ,BASE_INT
        ,DEFAULT_INT
        ,PAYMTHD_ID
        ,VOUCHTYPE_ID
        ,DEPOSITCURR_ID
        ,R_DEPOSITCURR
        ,EXPOSURE_AMT
        ,PAYHOLD_ID
        ,GUARTYPE_ID
        ,RESEACCT_ORG
        ,LOANACCT_ORG
        ,CLIENTACCT_ORG
        ,DEPOSITACCT_ORG
        ,FEEACC_ORG
        ,REVBRANCH_ID
        ,REVBRANCH_NM
        ,DEALSTART_TM
        ,DEALEND_TM
        ,DEAL_FG
        ,REGBRANCH_ID
        ,REVCIF_NM
        ,LOANTYPE_ID
        ,PROMISSORY_NO
        ,MNCONTRACT_NO
        ,CRAGREE_NO
        ,CONTRACT_AMT
        ,GUARN_NO
        ,BILL_AMT
        ,SUBJECT_NO
        ,SUBJECT2_NO
        ,SOURCEGRP
      FROM  MMAPST.ST_LOAN_TRANS_FLOW A
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
END P_DM_LOAN_TRANS_FLOW_HIS;
/
