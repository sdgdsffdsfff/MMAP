CREATE OR REPLACE PROCEDURE P_DM_CUST_BAL_HIS
(IO_STATUS OUT INTEGER,IO_SQLERR OUT VARCHAR2)
	AS
	TABLE_NAME VARCHAR2(125) :='DM_CUST_BAL_HIS';  -- ����(�޸�)
	PROCEDURE_NAME VARCHAR2(125) :='P_'||TABLE_NAME;  -- �洢������(�޸�)

	IO_ROW INTEGER; --��������
	ST_ROW INTEGER;  --Դ��������

	RETURN_NUM NUMBER;                -- ����״ֵ̬
	RETURN_STATUS VARCHAR2(5000);          -- ���ص�����

	V_ETL_DATE NUMBER;  -- ��������
	V_START_TIMESTAMP TIMESTAMP;  -- ���ؿ�ʼʱ��
	V_END_TIMESTAMP    TIMESTAMP;  -- ���ؽ���ʱ��

	DM_SQL VARCHAR2(20000);  -- ���SQL���

	DM_TODAY NUMBER;    -- ��������"����"
	DM_MAX_DATE NUMBER := 99991231;  -- �������

	TMP_PRE  VARCHAR(35);  -- ǰһ��������ʱ��
	TMP_CUR  VARCHAR(35);  -- ����������ʱ��
	TMP_INS  VARCHAR(35);  -- �²���������ʱ��
	TMP_UPD  VARCHAR(35);  -- ����������ʱ��

BEGIN

	SELECT SYSDATE INTO V_START_TIMESTAMP FROM DUAL;  -- ���س������п�ʼʱ��

	SELECT TO_NUMBER(TO_CHAR((SYSDATE),'YYYYMMDD')) INTO V_ETL_DATE FROM DUAL;  -- ȡϵͳ������Ϊ��������

	SELECT TX_DATE INTO DM_TODAY FROM MMAPST.ST_SYSTEM_DATE;  -- ȡ��������

	--��ѯST������Ƿ���'����'����
	SELECT COUNT(1) INTO ST_ROW FROM MMAPST.ST_CUST_BAL WHERE PERIOD_ID=DM_TODAY;

	--���ST����"����"���ݣ���������ݳ�ȡ�����򱣳�DM���������ݲ��䡣
	IF ST_ROW>0
	THEN
		IO_ROW:=0;
		-- ���ô�����ʱ��洢����
		P_MMAPDM_CREATE_TMP_TABLES(TABLE_NAME , RETURN_NUM , RETURN_STATUS,TMP_PRE,TMP_CUR,TMP_INS,TMP_UPD);

		--�ָ�����Ϊǰһ������״̬��Ϊ�������������������
		DELETE FROM  MMAPDM.DM_CUST_BAL_HIS   WHERE DM_START_DT = DM_TODAY;  -- ɾ����ʼ����Ϊ"����"������

		UPDATE MMAPDM.DM_CUST_BAL_HIS SET DM_END_DT= DM_MAX_DATE  WHERE DM_END_DT=  DM_TODAY;  -- ���½�������Ϊ"����"������Ϊ�������

		COMMIT;

		/* ȡǰһ������  */
		DM_SQL := '  INSERT INTO  '||  TMP_PRE  || '
		(
		  TX_DATE          --  ��������
		  ,PERIOD_ID        --  ����
		  ,CUSTOMER_ID      --  �ͻ���
		  ,CUST_CD_BAL_LC      --  ���ڴ��������ң�
		  ,CUST_TD_BAL_LC      --  ���ڴ��������ң�
		  ,CUST_LOAN_BAL_LC    --  ����������ң�
		  ,CUST_NDEBT_BAL_LC    --  ��ծ������ң�
		  ,CUST_FOUND_BAL_LC    --  ����������ң�
		  ,CUST_FIN_BAL_LC    --  ���������ң�
		  ,CUST_INSURE_BAL_LC    --  ����������ң�
		  ,CUST_NOBLE_BAL_LC    --  �����������ң�
		  ,CUST_CREDIT_BAL_LC    --  ���ÿ����Ѷ����ң�
		  ,CUST_ALL_BAL_LC    --  �ͻ����ʲ�������ң�
		  ,CUST_CD_BAL_FC      --  ���ڴ��������ۺ�����ң�
		  ,CUST_TD_BAL_FC      --  ���ڴ��������ۺ�����ң�
		  ,CUST_LOAN_BAL_FC    --  ����������ۺ�����ң�
		  ,CUST_CREDIT_BAL_FC    --  ���ÿ����Ѷ����ۺ�����ң�
		  ,CUST_ALL_BAL_FC    --  �ͻ����ʲ�������ۺ�����ң�
		  ,CUST_ALL_BAL      --  �ͻ����ʲ��������+��ң�
		  ,ETL_DATE        --  ��������
		  ,DM_START_DT      --  ��ʼ����
		  ,DM_END_DT        --  ��������

		)
		SELECT
		  TX_DATE          --  ��������
		  ,PERIOD_ID        --  ����
		  ,CUSTOMER_ID      --  �ͻ���
		  ,CUST_CD_BAL_LC      --  ���ڴ��������ң�
		  ,CUST_TD_BAL_LC      --  ���ڴ��������ң�
		  ,CUST_LOAN_BAL_LC    --  ����������ң�
		  ,CUST_NDEBT_BAL_LC    --  ��ծ������ң�
		  ,CUST_FOUND_BAL_LC    --  ����������ң�
		  ,CUST_FIN_BAL_LC    --  ���������ң�
		  ,CUST_INSURE_BAL_LC    --  ����������ң�
		  ,CUST_NOBLE_BAL_LC    --  �����������ң�
		  ,CUST_CREDIT_BAL_LC    --  ���ÿ����Ѷ����ң�
		  ,CUST_ALL_BAL_LC    --  �ͻ����ʲ�������ң�
		  ,CUST_CD_BAL_FC      --  ���ڴ��������ۺ�����ң�
		  ,CUST_TD_BAL_FC      --  ���ڴ��������ۺ�����ң�
		  ,CUST_LOAN_BAL_FC    --  ����������ۺ�����ң�
		  ,CUST_CREDIT_BAL_FC    --  ���ÿ����Ѷ����ۺ�����ң�
		  ,CUST_ALL_BAL_FC    --  �ͻ����ʲ�������ۺ�����ң�
		  ,CUST_ALL_BAL      --  �ͻ����ʲ��������+��ң�
		  ,ETL_DATE        --  ��������
		  ,DM_START_DT      --  ��ʼ����
		  ,DM_END_DT        --  ��������
		FROM MMAPDM.DM_CUST_BAL_HIS
		WHERE DM_END_DT  = '  || DM_MAX_DATE ;
		EXECUTE  IMMEDIATE DM_SQL;
		COMMIT;

		/*ȡ��������*/
		DM_SQL := 'INSERT INTO '|| TMP_CUR || '
		(
		  TX_DATE          --  ��������
		  ,PERIOD_ID        --  ����
		  ,CUSTOMER_ID      --  �ͻ���
		  ,CUST_CD_BAL_LC      --  ���ڴ��������ң�
		  ,CUST_TD_BAL_LC      --  ���ڴ��������ң�
		  ,CUST_LOAN_BAL_LC    --  ����������ң�
		  ,CUST_NDEBT_BAL_LC    --  ��ծ������ң�
		  ,CUST_FOUND_BAL_LC    --  ����������ң�
		  ,CUST_FIN_BAL_LC    --  ���������ң�
		  ,CUST_INSURE_BAL_LC    --  ����������ң�
		  ,CUST_NOBLE_BAL_LC    --  �����������ң�
		  ,CUST_CREDIT_BAL_LC    --  ���ÿ����Ѷ����ң�
		  ,CUST_ALL_BAL_LC    --  �ͻ����ʲ�������ң�
		  ,CUST_CD_BAL_FC      --  ���ڴ��������ۺ�����ң�
		  ,CUST_TD_BAL_FC      --  ���ڴ��������ۺ�����ң�
		  ,CUST_LOAN_BAL_FC    --  ����������ۺ�����ң�
		  ,CUST_CREDIT_BAL_FC    --  ���ÿ����Ѷ����ۺ�����ң�
		  ,CUST_ALL_BAL_FC    --  �ͻ����ʲ�������ۺ�����ң�
		  ,CUST_ALL_BAL      --  �ͻ����ʲ��������+��ң�
		  ,ETL_DATE        --  ��������
		  ,DM_START_DT      --  ��ʼ����
		  ,DM_END_DT        --  ��������
		)
		SELECT
		  TX_DATE          --  ��������
		  ,PERIOD_ID        --  ����
		  ,CUSTOMER_ID      --  �ͻ���
		  ,CUST_CD_BAL_LC      --  ���ڴ��������ң�
		  ,CUST_TD_BAL_LC      --  ���ڴ��������ң�
		  ,CUST_LOAN_BAL_LC    --  ����������ң�
		  ,CUST_NDEBT_BAL_LC    --  ��ծ������ң�
		  ,CUST_FOUND_BAL_LC    --  ����������ң�
		  ,CUST_FIN_BAL_LC    --  ���������ң�
		  ,CUST_INSURE_BAL_LC    --  ����������ң�
		  ,CUST_NOBLE_BAL_LC    --  �����������ң�
		  ,CUST_CREDIT_BAL_LC    --  ���ÿ����Ѷ����ң�
		  ,CUST_ALL_BAL_LC    --  �ͻ����ʲ�������ң�
		  ,CUST_CD_BAL_FC      --  ���ڴ��������ۺ�����ң�
		  ,CUST_TD_BAL_FC      --  ���ڴ��������ۺ�����ң�
		  ,CUST_LOAN_BAL_FC    --  ����������ۺ�����ң�
		  ,CUST_CREDIT_BAL_FC    --  ���ÿ����Ѷ����ۺ�����ң�
		  ,CUST_ALL_BAL_FC    --  �ͻ����ʲ�������ۺ�����ң�
		  ,CUST_ALL_BAL      --  �ͻ����ʲ��������+��ң�
		  ,'||  V_ETL_DATE  ||'
		  ,'||  DM_TODAY ||  '
		  ,'||  DM_MAX_DATE  ||'
		FROM MMAPST.ST_CUST_BAL';
		EXECUTE  IMMEDIATE DM_SQL;
		COMMIT;

		/*cur-pre =  �������ݣ������޸ĺ�*/
		DM_SQL := 'INSERT INTO '|| TMP_INS || '
		(
		  TX_DATE          --  ��������
		  ,PERIOD_ID        --  ����
		  ,CUSTOMER_ID      --  �ͻ���
		  ,CUST_CD_BAL_LC      --  ���ڴ��������ң�
		  ,CUST_TD_BAL_LC      --  ���ڴ��������ң�
		  ,CUST_LOAN_BAL_LC    --  ����������ң�
		  ,CUST_NDEBT_BAL_LC    --  ��ծ������ң�
		  ,CUST_FOUND_BAL_LC    --  ����������ң�
		  ,CUST_FIN_BAL_LC    --  ���������ң�
		  ,CUST_INSURE_BAL_LC    --  ����������ң�
		  ,CUST_NOBLE_BAL_LC    --  �����������ң�
		  ,CUST_CREDIT_BAL_LC    --  ���ÿ����Ѷ����ң�
		  ,CUST_ALL_BAL_LC    --  �ͻ����ʲ�������ң�
		  ,CUST_CD_BAL_FC      --  ���ڴ��������ۺ�����ң�
		  ,CUST_TD_BAL_FC      --  ���ڴ��������ۺ�����ң�
		  ,CUST_LOAN_BAL_FC    --  ����������ۺ�����ң�
		  ,CUST_CREDIT_BAL_FC    --  ���ÿ����Ѷ����ۺ�����ң�
		  ,CUST_ALL_BAL_FC    --  �ͻ����ʲ�������ۺ�����ң�
		  ,CUST_ALL_BAL      --  �ͻ����ʲ��������+��ң�
		  ,ETL_DATE        --  ��������
		  ,DM_START_DT      --  ��ʼ����
		  ,DM_END_DT        --  ��������
		)
		SELECT
		  TX_DATE          --  ��������
		  ,PERIOD_ID        --  ����
		  ,CUSTOMER_ID      --  �ͻ���
		  ,CUST_CD_BAL_LC      --  ���ڴ��������ң�
		  ,CUST_TD_BAL_LC      --  ���ڴ��������ң�
		  ,CUST_LOAN_BAL_LC    --  ����������ң�
		  ,CUST_NDEBT_BAL_LC    --  ��ծ������ң�
		  ,CUST_FOUND_BAL_LC    --  ����������ң�
		  ,CUST_FIN_BAL_LC    --  ���������ң�
		  ,CUST_INSURE_BAL_LC    --  ����������ң�
		  ,CUST_NOBLE_BAL_LC    --  �����������ң�
		  ,CUST_CREDIT_BAL_LC    --  ���ÿ����Ѷ����ң�
		  ,CUST_ALL_BAL_LC    --  �ͻ����ʲ�������ң�
		  ,CUST_CD_BAL_FC      --  ���ڴ��������ۺ�����ң�
		  ,CUST_TD_BAL_FC      --  ���ڴ��������ۺ�����ң�
		  ,CUST_LOAN_BAL_FC    --  ����������ۺ�����ң�
		  ,CUST_CREDIT_BAL_FC    --  ���ÿ����Ѷ����ۺ�����ң�
		  ,CUST_ALL_BAL_FC    --  �ͻ����ʲ�������ۺ�����ң�
		  ,CUST_ALL_BAL      --  �ͻ����ʲ��������+��ң�
		  ,ETL_DATE        --  ��������
		  ,DM_START_DT      --  ��ʼ����
		  ,DM_END_DT        --  ��������
		FROM '|| TMP_CUR ||  '  a
		WHERE
		NOT  EXISTS
		(
		  SELECT
		    CUSTOMER_ID        --  �ͻ���
		    ,CUST_CD_BAL_LC      --  ���ڴ��������ң�
		    ,CUST_TD_BAL_LC      --  ���ڴ��������ң�
		    ,CUST_LOAN_BAL_LC    --  ����������ң�
		    ,CUST_NDEBT_BAL_LC    --  ��ծ������ң�
		    ,CUST_FOUND_BAL_LC    --  ����������ң�
		    ,CUST_FIN_BAL_LC    --  ���������ң�
		    ,CUST_INSURE_BAL_LC    --  ����������ң�
		    ,CUST_NOBLE_BAL_LC    --  �����������ң�
		    ,CUST_CREDIT_BAL_LC    --  ���ÿ����Ѷ����ң�
		    ,CUST_ALL_BAL_LC    --  �ͻ����ʲ�������ң�
		    ,CUST_CD_BAL_FC      --  ���ڴ��������ۺ�����ң�
		    ,CUST_TD_BAL_FC      --  ���ڴ��������ۺ�����ң�
		    ,CUST_LOAN_BAL_FC    --  ����������ۺ�����ң�
		    ,CUST_CREDIT_BAL_FC    --  ���ÿ����Ѷ����ۺ�����ң�
		    ,CUST_ALL_BAL_FC    --  �ͻ����ʲ�������ۺ�����ң�
		    ,CUST_ALL_BAL      --  �ͻ����ʲ��������+��ң�
		  FROM '|| TMP_PRE ||  '  b
		  WHERE
		    a.CUSTOMER_ID= b.CUSTOMER_ID
		  AND  NVL(a.CUST_CD_BAL_LC,0)= NVL(b.CUST_CD_BAL_LC,0)
		  AND  NVL(a.CUST_TD_BAL_LC,0)= NVL(b.CUST_TD_BAL_LC,0)
		  AND  NVL(a.CUST_LOAN_BAL_LC,0)=  NVL(b.CUST_LOAN_BAL_LC,0)
		  AND  NVL(a.CUST_NDEBT_BAL_LC,0)= NVL(b.CUST_NDEBT_BAL_LC,0)
		  AND  NVL(a.CUST_FOUND_BAL_LC,0)= NVL(b.CUST_FOUND_BAL_LC,0)
		  AND  NVL(a.CUST_FIN_BAL_LC,0)=  NVL(b.CUST_FIN_BAL_LC,0)
		  AND  NVL(a.CUST_INSURE_BAL_LC,0)= NVL(b.CUST_INSURE_BAL_LC,0)
		  AND  NVL(a.CUST_NOBLE_BAL_LC,0)= NVL(b.CUST_NOBLE_BAL_LC,0)
		  AND  NVL(a.CUST_CREDIT_BAL_LC,0)=  NVL(b.CUST_CREDIT_BAL_LC,0)
		  AND  NVL(a.CUST_ALL_BAL_LC,0)= NVL(b.CUST_ALL_BAL_LC,0)
		  AND  NVL(a.CUST_CD_BAL_FC,0)= NVL(b.CUST_CD_BAL_FC,0)
		  AND  NVL(a.CUST_TD_BAL_FC,0)=  NVL(b.CUST_TD_BAL_FC,0)
		  AND  NVL(a.CUST_LOAN_BAL_FC,0)= NVL(b.CUST_LOAN_BAL_FC,0)
		  AND  NVL(a.CUST_CREDIT_BAL_FC,0)= NVL(b.CUST_CREDIT_BAL_FC,0)
		  AND  NVL(a.CUST_ALL_BAL_FC,0)=  NVL(b.CUST_ALL_BAL_FC,0)
		  AND  NVL(a.CUST_ALL_BAL,0)= NVL(b.CUST_ALL_BAL,0)
		)';
		EXECUTE  IMMEDIATE DM_SQL;
		COMMIT;

		 /*  pre-cur  = ɾ���ݣ������޸�ǰ�� */
		DM_SQL := 'INSERT INTO '|| TMP_UPD || '
		(
		  TX_DATE          --  ��������
		  ,PERIOD_ID        --  ����
		  ,CUSTOMER_ID      --  �ͻ���
		  ,CUST_CD_BAL_LC      --  ���ڴ��������ң�
		  ,CUST_TD_BAL_LC      --  ���ڴ��������ң�
		  ,CUST_LOAN_BAL_LC    --  ����������ң�
		  ,CUST_NDEBT_BAL_LC    --  ��ծ������ң�
		  ,CUST_FOUND_BAL_LC    --  ����������ң�
		  ,CUST_FIN_BAL_LC    --  ���������ң�
		  ,CUST_INSURE_BAL_LC    --  ����������ң�
		  ,CUST_NOBLE_BAL_LC    --  �����������ң�
		  ,CUST_CREDIT_BAL_LC    --  ���ÿ����Ѷ����ң�
		  ,CUST_ALL_BAL_LC    --  �ͻ����ʲ�������ң�
		  ,CUST_CD_BAL_FC      --  ���ڴ��������ۺ�����ң�
		  ,CUST_TD_BAL_FC      --  ���ڴ��������ۺ�����ң�
		  ,CUST_LOAN_BAL_FC    --  ����������ۺ�����ң�
		  ,CUST_CREDIT_BAL_FC    --  ���ÿ����Ѷ����ۺ�����ң�
		  ,CUST_ALL_BAL_FC    --  �ͻ����ʲ�������ۺ�����ң�
		  ,CUST_ALL_BAL      --  �ͻ����ʲ��������+��ң�
		  ,ETL_DATE        --  ��������
		  ,DM_START_DT      --  ��ʼ����
		  ,DM_END_DT        --  ��������
		)
		SELECT
		  TX_DATE          --  ��������
		  ,PERIOD_ID        --  ����
		  ,CUSTOMER_ID      --  �ͻ���
		  ,CUST_CD_BAL_LC      --  ���ڴ��������ң�
		  ,CUST_TD_BAL_LC      --  ���ڴ��������ң�
		  ,CUST_LOAN_BAL_LC    --  ����������ң�
		  ,CUST_NDEBT_BAL_LC    --  ��ծ������ң�
		  ,CUST_FOUND_BAL_LC    --  ����������ң�
		  ,CUST_FIN_BAL_LC    --  ���������ң�
		  ,CUST_INSURE_BAL_LC    --  ����������ң�
		  ,CUST_NOBLE_BAL_LC    --  �����������ң�
		  ,CUST_CREDIT_BAL_LC    --  ���ÿ����Ѷ����ң�
		  ,CUST_ALL_BAL_LC    --  �ͻ����ʲ�������ң�
		  ,CUST_CD_BAL_FC      --  ���ڴ��������ۺ�����ң�
		  ,CUST_TD_BAL_FC      --  ���ڴ��������ۺ�����ң�
		  ,CUST_LOAN_BAL_FC    --  ����������ۺ�����ң�
		  ,CUST_CREDIT_BAL_FC    --  ���ÿ����Ѷ����ۺ�����ң�
		  ,CUST_ALL_BAL_FC    --  �ͻ����ʲ�������ۺ�����ң�
		  ,CUST_ALL_BAL      --  �ͻ����ʲ��������+��ң�
		  ,ETL_DATE        --  ��������
		  ,DM_START_DT      --  ��ʼ����
		  ,DM_END_DT        --  ��������
		FROM '|| TMP_PRE ||' a
		WHERE
		NOT  EXISTS
		(
		  SELECT
		    TX_DATE          --  ��������
		    ,PERIOD_ID        --  ����
		    ,CUSTOMER_ID      --  �ͻ���
		    ,CUST_CD_BAL_LC      --  ���ڴ��������ң�
		    ,CUST_TD_BAL_LC      --  ���ڴ��������ң�
		    ,CUST_LOAN_BAL_LC    --  ����������ң�
		    ,CUST_NDEBT_BAL_LC    --  ��ծ������ң�
		    ,CUST_FOUND_BAL_LC    --  ����������ң�
		    ,CUST_FIN_BAL_LC    --  ���������ң�
		    ,CUST_INSURE_BAL_LC    --  ����������ң�
		    ,CUST_NOBLE_BAL_LC    --  �����������ң�
		    ,CUST_CREDIT_BAL_LC    --  ���ÿ����Ѷ����ң�
		    ,CUST_ALL_BAL_LC    --  �ͻ����ʲ�������ң�
		    ,CUST_CD_BAL_FC      --  ���ڴ��������ۺ�����ң�
		    ,CUST_TD_BAL_FC      --  ���ڴ��������ۺ�����ң�
		    ,CUST_LOAN_BAL_FC    --  ����������ۺ�����ң�
		    ,CUST_CREDIT_BAL_FC    --  ���ÿ����Ѷ����ۺ�����ң�
		    ,CUST_ALL_BAL_FC    --  �ͻ����ʲ�������ۺ�����ң�
		    ,CUST_ALL_BAL      --  �ͻ����ʲ��������+��ң�
		    ,ETL_DATE        --  ��������
		    ,DM_START_DT      --  ��ʼ����
		    ,DM_END_DT        --  ��������
		  FROM '|| TMP_CUR ||  '  b
		  WHERE
		    a.CUSTOMER_ID= b.CUSTOMER_ID
		  AND  NVL(a.CUST_CD_BAL_LC,0)= NVL(b.CUST_CD_BAL_LC,0)
		  AND  NVL(a.CUST_TD_BAL_LC,0)= NVL(b.CUST_TD_BAL_LC,0)
		  AND  NVL(a.CUST_LOAN_BAL_LC,0)=  NVL(b.CUST_LOAN_BAL_LC,0)
		  AND  NVL(a.CUST_NDEBT_BAL_LC,0)= NVL(b.CUST_NDEBT_BAL_LC,0)
		  AND  NVL(a.CUST_FOUND_BAL_LC,0)= NVL(b.CUST_FOUND_BAL_LC,0)
		  AND  NVL(a.CUST_FIN_BAL_LC,0)=  NVL(b.CUST_FIN_BAL_LC,0)
		  AND  NVL(a.CUST_INSURE_BAL_LC,0)= NVL(b.CUST_INSURE_BAL_LC,0)
		  AND  NVL(a.CUST_NOBLE_BAL_LC,0)= NVL(b.CUST_NOBLE_BAL_LC,0)
		  AND  NVL(a.CUST_CREDIT_BAL_LC,0)=  NVL(b.CUST_CREDIT_BAL_LC,0)
		  AND  NVL(a.CUST_ALL_BAL_LC,0)= NVL(b.CUST_ALL_BAL_LC,0)
		  AND  NVL(a.CUST_CD_BAL_FC,0)= NVL(b.CUST_CD_BAL_FC,0)
		  AND  NVL(a.CUST_TD_BAL_FC,0)=  NVL(b.CUST_TD_BAL_FC,0)
		  AND  NVL(a.CUST_LOAN_BAL_FC,0)= NVL(b.CUST_LOAN_BAL_FC,0)
		  AND  NVL(a.CUST_CREDIT_BAL_FC,0)= NVL(b.CUST_CREDIT_BAL_FC,0)
		  AND  NVL(a.CUST_ALL_BAL_FC,0)=  NVL(b.CUST_ALL_BAL_FC,0)
		  AND  NVL(a.CUST_ALL_BAL,0)= NVL(b.CUST_ALL_BAL,0)
		)';
		EXECUTE  IMMEDIATE DM_SQL;
		COMMIT;

		/* ������ʷ���������ݵĽ�ֹʱ��Ϊ����ʱ��(ɾ��-�����������) */
		DM_SQL := 'DELETE FROM MMAPDM.DM_CUST_BAL_HIS A
		WHERE
		EXISTS(
		SELECT
		  TX_DATE          --  ��������
		  ,PERIOD_ID        --  ����
		  ,CUSTOMER_ID      --  �ͻ���
		  ,CUST_CD_BAL_LC      --  ���ڴ��������ң�
		  ,CUST_TD_BAL_LC      --  ���ڴ��������ң�
		  ,CUST_LOAN_BAL_LC    --  ����������ң�
		  ,CUST_NDEBT_BAL_LC    --  ��ծ������ң�
		  ,CUST_FOUND_BAL_LC    --  ����������ң�
		  ,CUST_FIN_BAL_LC    --  ���������ң�
		  ,CUST_INSURE_BAL_LC    --  ����������ң�
		  ,CUST_NOBLE_BAL_LC    --  �����������ң�
		  ,CUST_CREDIT_BAL_LC    --  ���ÿ����Ѷ����ң�
		  ,CUST_ALL_BAL_LC    --  �ͻ����ʲ�������ң�
		  ,CUST_CD_BAL_FC      --  ���ڴ��������ۺ�����ң�
		  ,CUST_TD_BAL_FC      --  ���ڴ��������ۺ�����ң�
		  ,CUST_LOAN_BAL_FC    --  ����������ۺ�����ң�
		  ,CUST_CREDIT_BAL_FC    --  ���ÿ����Ѷ����ۺ�����ң�
		  ,CUST_ALL_BAL_FC    --  �ͻ����ʲ�������ۺ�����ң�
		  ,CUST_ALL_BAL      --  �ͻ����ʲ��������+��ң�
		  ,ETL_DATE        --  ��������
		  ,DM_START_DT      --  ��ʼ����
		  ,DM_END_DT        --  ��������
		FROM '|| TMP_UPD ||  ' b
		WHERE
		    a.CUSTOMER_ID= b.CUSTOMER_ID
		  AND  NVL(a.CUST_CD_BAL_LC,0)= NVL(b.CUST_CD_BAL_LC,0)
		  AND  NVL(a.CUST_TD_BAL_LC,0)= NVL(b.CUST_TD_BAL_LC,0)
		  AND  NVL(a.CUST_LOAN_BAL_LC,0)=  NVL(b.CUST_LOAN_BAL_LC,0)
		  AND  NVL(a.CUST_NDEBT_BAL_LC,0)= NVL(b.CUST_NDEBT_BAL_LC,0)
		  AND  NVL(a.CUST_FOUND_BAL_LC,0)= NVL(b.CUST_FOUND_BAL_LC,0)
		  AND  NVL(a.CUST_FIN_BAL_LC,0)=  NVL(b.CUST_FIN_BAL_LC,0)
		  AND  NVL(a.CUST_INSURE_BAL_LC,0)= NVL(b.CUST_INSURE_BAL_LC,0)
		  AND  NVL(a.CUST_NOBLE_BAL_LC,0)= NVL(b.CUST_NOBLE_BAL_LC,0)
		  AND  NVL(a.CUST_CREDIT_BAL_LC,0)=  NVL(b.CUST_CREDIT_BAL_LC,0)
		  AND  NVL(a.CUST_ALL_BAL_LC,0)= NVL(b.CUST_ALL_BAL_LC,0)
		  AND  NVL(a.CUST_CD_BAL_FC,0)= NVL(b.CUST_CD_BAL_FC,0)
		  AND  NVL(a.CUST_TD_BAL_FC,0)=  NVL(b.CUST_TD_BAL_FC,0)
		  AND  NVL(a.CUST_LOAN_BAL_FC,0)= NVL(b.CUST_LOAN_BAL_FC,0)
		  AND  NVL(a.CUST_CREDIT_BAL_FC,0)= NVL(b.CUST_CREDIT_BAL_FC,0)
		  AND  NVL(a.CUST_ALL_BAL_FC,0)=  NVL(b.CUST_ALL_BAL_FC,0)
		  AND  NVL(a.CUST_ALL_BAL,0)= NVL(b.CUST_ALL_BAL,0)
		)
		AND A.DM_END_DT='|| DM_MAX_DATE ;
		EXECUTE  IMMEDIATE DM_SQL;
		IO_ROW := SQL%ROWCOUNT ;

		DM_SQL := 'INSERT INTO MMAPDM.DM_CUST_BAL_HIS
		(
		  TX_DATE          --  ��������
		  ,PERIOD_ID        --  ����
		  ,CUSTOMER_ID      --  �ͻ���
		  ,CUST_CD_BAL_LC      --  ���ڴ��������ң�
		  ,CUST_TD_BAL_LC      --  ���ڴ��������ң�
		  ,CUST_LOAN_BAL_LC    --  ����������ң�
		  ,CUST_NDEBT_BAL_LC    --  ��ծ������ң�
		  ,CUST_FOUND_BAL_LC    --  ����������ң�
		  ,CUST_FIN_BAL_LC    --  ���������ң�
		  ,CUST_INSURE_BAL_LC    --  ����������ң�
		  ,CUST_NOBLE_BAL_LC    --  �����������ң�
		  ,CUST_CREDIT_BAL_LC    --  ���ÿ����Ѷ����ң�
		  ,CUST_ALL_BAL_LC    --  �ͻ����ʲ�������ң�
		  ,CUST_CD_BAL_FC      --  ���ڴ��������ۺ�����ң�
		  ,CUST_TD_BAL_FC      --  ���ڴ��������ۺ�����ң�
		  ,CUST_LOAN_BAL_FC    --  ����������ۺ�����ң�
		  ,CUST_CREDIT_BAL_FC    --  ���ÿ����Ѷ����ۺ�����ң�
		  ,CUST_ALL_BAL_FC    --  �ͻ����ʲ�������ۺ�����ң�
		  ,CUST_ALL_BAL      --  �ͻ����ʲ��������+��ң�
		  ,ETL_DATE        --  ��������
		  ,DM_START_DT      --  ��ʼ����
		  ,DM_END_DT        --  ��������
		)
		SELECT
		  TX_DATE          --  ��������
		  ,PERIOD_ID        --  ����
		  ,CUSTOMER_ID      --  �ͻ���
		  ,CUST_CD_BAL_LC      --  ���ڴ��������ң�
		  ,CUST_TD_BAL_LC      --  ���ڴ��������ң�
		  ,CUST_LOAN_BAL_LC    --  ����������ң�
		  ,CUST_NDEBT_BAL_LC    --  ��ծ������ң�
		  ,CUST_FOUND_BAL_LC    --  ����������ң�
		  ,CUST_FIN_BAL_LC    --  ���������ң�
		  ,CUST_INSURE_BAL_LC    --  ����������ң�
		  ,CUST_NOBLE_BAL_LC    --  �����������ң�
		  ,CUST_CREDIT_BAL_LC    --  ���ÿ����Ѷ����ң�
		  ,CUST_ALL_BAL_LC    --  �ͻ����ʲ�������ң�
		  ,CUST_CD_BAL_FC      --  ���ڴ��������ۺ�����ң�
		  ,CUST_TD_BAL_FC      --  ���ڴ��������ۺ�����ң�
		  ,CUST_LOAN_BAL_FC    --  ����������ۺ�����ң�
		  ,CUST_CREDIT_BAL_FC    --  ���ÿ����Ѷ����ۺ�����ң�
		  ,CUST_ALL_BAL_FC    --  �ͻ����ʲ�������ۺ�����ң�
		  ,CUST_ALL_BAL      --  �ͻ����ʲ��������+��ң�
		  ,ETL_DATE        --  ��������
		  ,DM_START_DT      --  ��ʼ����
		  ,'|| DM_TODAY ||'
		FROM '|| TMP_UPD ||  '';
		EXECUTE  IMMEDIATE DM_SQL;
		IO_ROW := IO_ROW+SQL%ROWCOUNT ;

		/* ������������  */
		DM_SQL := 'INSERT INTO  MMAPDM.DM_CUST_BAL_HIS
		(
		  TX_DATE          --  ��������
		  ,PERIOD_ID        --  ����
		  ,CUSTOMER_ID      --  �ͻ���
		  ,CUST_CD_BAL_LC      --  ���ڴ��������ң�
		  ,CUST_TD_BAL_LC      --  ���ڴ��������ң�
		  ,CUST_LOAN_BAL_LC    --  ����������ң�
		  ,CUST_NDEBT_BAL_LC    --  ��ծ������ң�
		  ,CUST_FOUND_BAL_LC    --  ����������ң�
		  ,CUST_FIN_BAL_LC    --  ���������ң�
		  ,CUST_INSURE_BAL_LC    --  ����������ң�
		  ,CUST_NOBLE_BAL_LC    --  �����������ң�
		  ,CUST_CREDIT_BAL_LC    --  ���ÿ����Ѷ����ң�
		  ,CUST_ALL_BAL_LC    --  �ͻ����ʲ�������ң�
		  ,CUST_CD_BAL_FC      --  ���ڴ��������ۺ�����ң�
		  ,CUST_TD_BAL_FC      --  ���ڴ��������ۺ�����ң�
		  ,CUST_LOAN_BAL_FC    --  ����������ۺ�����ң�
		  ,CUST_CREDIT_BAL_FC    --  ���ÿ����Ѷ����ۺ�����ң�
		  ,CUST_ALL_BAL_FC    --  �ͻ����ʲ�������ۺ�����ң�
		  ,CUST_ALL_BAL      --  �ͻ����ʲ��������+��ң�
		  ,ETL_DATE        --  ��������
		  ,DM_START_DT      --  ��ʼ����
		  ,DM_END_DT        --  ��������
		)
		SELECT
		  TX_DATE          --  ��������
		  ,PERIOD_ID        --  ����
		  ,CUSTOMER_ID      --  �ͻ���
		  ,CUST_CD_BAL_LC      --  ���ڴ��������ң�
		  ,CUST_TD_BAL_LC      --  ���ڴ��������ң�
		  ,CUST_LOAN_BAL_LC    --  ����������ң�
		  ,CUST_NDEBT_BAL_LC    --  ��ծ������ң�
		  ,CUST_FOUND_BAL_LC    --  ����������ң�
		  ,CUST_FIN_BAL_LC    --  ���������ң�
		  ,CUST_INSURE_BAL_LC    --  ����������ң�
		  ,CUST_NOBLE_BAL_LC    --  �����������ң�
		  ,CUST_CREDIT_BAL_LC    --  ���ÿ����Ѷ����ң�
		  ,CUST_ALL_BAL_LC    --  �ͻ����ʲ�������ң�
		  ,CUST_CD_BAL_FC      --  ���ڴ��������ۺ�����ң�
		  ,CUST_TD_BAL_FC      --  ���ڴ��������ۺ�����ң�
		  ,CUST_LOAN_BAL_FC    --  ����������ۺ�����ң�
		  ,CUST_CREDIT_BAL_FC    --  ���ÿ����Ѷ����ۺ�����ң�
		  ,CUST_ALL_BAL_FC    --  �ͻ����ʲ�������ۺ�����ң�
		  ,CUST_ALL_BAL      --  �ͻ����ʲ��������+��ң�
      ,ETL_DATE        --  ��������
      ,DM_START_DT      --  ��ʼ����
      ,DM_END_DT        --  ��������
    FROM '|| TMP_INS ||  '';
    EXECUTE  IMMEDIATE DM_SQL;

    IO_ROW := IO_ROW+SQL%ROWCOUNT ;

    P_MMAPDM_DROP_TMP_TABLES(TABLE_NAME,RETURN_NUM,RETURN_STATUS);
  END IF;

  SELECT SYSDATE INTO  V_END_TIMESTAMP  FROM dual;  -- ���س������н���ʱ��

  /*д��־*/
  IO_STATUS := 0 ;
  IO_SQLERR := 'SUSSCESS';
  P_MMAPDM_WRITE_LOGS(PROCEDURE_NAME,IO_STATUS,IO_ROW,V_START_TIMESTAMP,V_END_TIMESTAMP,IO_SQLERR);
  COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
  ROLLBACK ;
  IO_STATUS := 9 ;
  IO_SQLERR := SQLCODE ||  SQLERRM  ;
  SELECT SYSDATE INTO  V_END_TIMESTAMP  FROM dual;
  P_MMAPDM_WRITE_LOGS(PROCEDURE_NAME,IO_STATUS,IO_ROW,V_START_TIMESTAMP,V_END_TIMESTAMP,IO_SQLERR);
END  P_DM_CUST_BAL_HIS;
/
