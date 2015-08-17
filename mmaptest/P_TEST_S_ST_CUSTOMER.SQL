CREATE OR REPLACE PROCEDURE "P_TEST_S_ST_CUSTOMER"

  AS
TRANS_NAME VARCHAR2(50) :='P_TEST_S_ST_CUSTOMER' ; --�洢������

V_TABLE_NAME VARCHAR2(50) :='ST_CUSTOMER';  -- ����Ŀ�����
V_COL VARCHAR2(50);     --�ֶ�Ӣ����
V_COL_NAME VARCHAR2(50);     --�ֶ�������
V_COL_DES VARCHAR2(50);     --�ֶ�����
V_TEST_DES VARCHAR2(50);     --��������
V_TEST_STAGE VARCHAR2(50) :='S_ST';  -- ���Խ׶�
V_TEST_TIMESTAMP TIMESTAMP;  -- ���ز���ʱ��

--�洢������־����
V_START_TIMESTAMP TIMESTAMP;  -- ���ؿ�ʼʱ��
V_END_TIMESTAMP    TIMESTAMP;  -- ���ؽ���ʱ��
IO_ROW INTEGER;
IO_STATUS INTEGER;
VO_SQLERR VARCHAR2(5000);          -- ���ص�����

BEGIN
SELECT SYSDATE INTO V_START_TIMESTAMP FROM dual;  -- ���س������п�ʼʱ��
V_TEST_TIMESTAMP := V_START_TIMESTAMP;

--1.�ַ����ֶ�
--������е��ַ�������û�����⴦����������Ա���¼������;��������⴦���������������ֶ��߼����ԡ�
--����¼��
V_COL := 'TABLE_COUNT';
V_COL_NAME := '��¼��';
V_COL_DES := '���м�¼��';
V_TEST_DES := '����';

INSERT INTO MMAPTEST.MMAP_TEST
  (
    TEST_DATE          --  ����ʱ��
    ,TEST_STAGE        --  ���Խ׶�
    ,TABLE_NAME      --  Ŀ�����
    ,TX_DATE      --  ��������
    ,COL      -- �ֶ�Ӣ����
    ,COL_NAME    --  �ֶ�������
    ,COL_DES    --  �ֶ�����
    ,TEST_DES    --  ��������
    ,VALUE_SOURCE    -- �ֶ�ֵ��Դ��
    ,VALUE_TARGET    --  �ֶ�ֵ��Ŀ�꣩
    ,COMPARE_RESULT    --  �ԱȽ��
    ,REMARK    --  ��ע

  )
select
V_TEST_TIMESTAMP
,V_TEST_STAGE
,V_TABLE_NAME
,COALESCE(T1.tx_dt,T2.TX_DATE)
,V_COL
,V_COL_NAME
,V_COL_DES
,V_TEST_DES
,T1.S_NUM
,T2.ST_NUM
,NVL(T1.S_NUM,0)-nvl(T2.ST_NUM,0)
,NULL
FROM
(
select A.Tx_DT,COUNT(1) AS S_NUM    --�޸�
from   MMAPST.DMMKT_CSMAST A
GROUP BY Tx_DT
) T1
FULL JOIN
(
SELECT A.TX_DATE,COUNT(1) AS ST_NUM  --�޸�
FROM   MMAPST.ST_CUSTOMER A
GROUP BY TX_DATE
)T2
ON T1.Tx_DT=T2.TX_DATE;

--2.��ֵ���ֶ�
--�Ծ������⴦����ÿ����ֵ���ֶ������ԣ�������и����ֶξ�δ�����⴦���������ȡ��һ����ֵ���ֶ������ԣ��������û�и����ֶΣ���������
--AGE
V_COL := 'AGE';
V_COL_NAME := '�ͻ�����';
V_COL_DES := '';
V_TEST_DES := '�����֤����';

INSERT INTO MMAPTEST.MMAP_TEST
  (
    TEST_DATE          --  ����ʱ��
    ,TEST_STAGE        --  ���Խ׶�
    ,TABLE_NAME      --  Ŀ�����
    ,TX_DATE      --  ��������
    ,COL      -- �ֶ�Ӣ����
    ,COL_NAME    --  �ֶ�������
    ,COL_DES    --  �ֶ�����
    ,TEST_DES    --  ��������
    ,VALUE_SOURCE    -- �ֶ�ֵ��Դ��
    ,VALUE_TARGET    --  �ֶ�ֵ��Ŀ�꣩
    ,COMPARE_RESULT    --  �ԱȽ��
    ,REMARK    --  ��ע
  )
select
V_TEST_TIMESTAMP
,V_TEST_STAGE
,V_TABLE_NAME
,COALESCE(T1.tx_dt,T2.TX_DATE)
,V_COL
,V_COL_NAME
,V_COL_DES
,V_TEST_DES
,T1.S_NUM
,T2.ST_NUM
,NVL(T1.S_NUM,0)-nvl(T2.ST_NUM,0)
,NULL
FROM
(
SELECT Tx_DT,SUM(N_AGE) AS S_NUM
from  MMAPST.DMMKT_CSMAST 
WHERE CIF_ORG IN (8000059309,8900045250,8600052623,3100019910)
GROUP BY Tx_DT
) T1
FULL JOIN
(
SELECT TX_DATE,SUM(AGE) AS ST_NUM
from  MMAPST.ST_CUSTOMER 
WHERE CUSTOMER_ID IN (8000059309,8900045250,8600052623,3100019910)
GROUP BY TX_DATE
)T2
ON T1.Tx_DT=T2.TX_DATE;


--2.��ֵ���ֶ�
--�Ծ������⴦����ÿ����ֵ���ֶ������ԣ�������и����ֶξ�δ�����⴦���������ȡ��һ����ֵ���ֶ������ԣ��������û�и����ֶΣ���������
--AGE
V_COL := 'EMP_IND';
V_COL_NAME := '�Ƿ���Ա';
V_COL_DES := '�Ƿ���Ա����ͳ��';
V_TEST_DES := '����';


INSERT INTO MMAPTEST.MMAP_TEST
  (
    TEST_DATE          --  ����ʱ��
    ,TEST_STAGE        --  ���Խ׶�
    ,TABLE_NAME      --  Ŀ�����
    ,TX_DATE      --  ��������
    ,COL      -- �ֶ�Ӣ����
    ,COL_NAME    --  �ֶ�������
    ,COL_DES    --  �ֶ�����
    ,TEST_DES    --  ��������
    ,VALUE_SOURCE    -- �ֶ�ֵ��Դ��
    ,VALUE_TARGET    --  �ֶ�ֵ��Ŀ�꣩
    ,COMPARE_RESULT    --  �ԱȽ��
    ,REMARK    --  ��ע
  )
select
V_TEST_TIMESTAMP
,V_TEST_STAGE
,V_TABLE_NAME
,COALESCE(T1.tx_dt,T2.TX_DATE)
,V_COL
,V_COL_NAME
,V_COL_DES
,V_TEST_DES
,T1.S_NUM
,T2.ST_NUM
,NVL(T1.S_NUM,0)-nvl(T2.ST_NUM,0)
,NULL
FROM
(
SELECT TX_DT,COUNT(*) AS S_NUM FROM MMAPST.DMMKT_CSMAST WHERE STAFF_FG='Y'
GROUP BY TX_DT
) T1
FULL JOIN
(
SELECT TX_DATE,COUNT(*) AS ST_NUM FROM MMAPDM.DM_CUSTOMER WHERE EMP_IND='1'
GROUP BY TX_DATE
)T2
ON T1.Tx_DT=T2.TX_DATE;



IO_ROW := SQL%ROWCOUNT ; --��¼�洢���̼�¼��
COMMIT;
SELECT SYSDATE INTO  V_END_TIMESTAMP  FROM dual;  -- ���س������н���ʱ��
IO_STATUS := 0 ;
VO_SQLERR := 'SUSSCESS';

P_TEST_WRITE_LOGS(TRANS_NAME,IO_STATUS,IO_ROW,V_START_TIMESTAMP,V_END_TIMESTAMP,VO_SQLERR);

EXCEPTION
    WHEN OTHERS THEN
  ROLLBACK ;
  IO_STATUS := 9 ;
  VO_SQLERR := SQLCODE ||  SQLERRM  ;
  SELECT SYSDATE INTO  V_END_TIMESTAMP  FROM dual;
  P_TEST_WRITE_LOGS(TRANS_NAME,IO_STATUS,IO_ROW,V_START_TIMESTAMP,V_END_TIMESTAMP,VO_SQLERR);



END  P_TEST_S_ST_CUSTOMER;
/