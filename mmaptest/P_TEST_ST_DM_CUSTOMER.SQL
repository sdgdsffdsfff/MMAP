CREATE OR REPLACE PROCEDURE "P_TEST_ST_DM_CUSTOMER"

  AS
TRANS_NAME VARCHAR2(50) :='P_TEST_ST_DM_CUSTOMER' ; --�洢������

V_TABLE_NAME VARCHAR2(50) :='DM_CUSTOMER';  -- ����Ŀ�����
V_COL VARCHAR2(50);     --�ֶ�Ӣ����
V_COL_NAME VARCHAR2(50);     --�ֶ�������
V_COL_DES VARCHAR2(50);     --�ֶ�����
V_TEST_DES VARCHAR2(50);     --��������
V_TEST_STAGE VARCHAR2(50) :='ST_DM';  -- ���Խ׶�
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
,COALESCE(T1.TX_DATE,T2.TX_DATE)
,V_COL
,V_COL_NAME
,V_COL_DES
,V_TEST_DES
,T1.ST_NUM
,T2.DM_NUM
,NVL(T1.ST_NUM,0)-nvl(T2.DM_NUM,0)
,NULL
FROM
(
select A.TX_DATE,COUNT(1) AS ST_NUM   
from    MMAPST.ST_CUSTOMER A
GROUP BY TX_DATE
) T1
FULL JOIN
(
SELECT A.TX_DATE,COUNT(1) AS DM_NUM  
FROM   MMAPDM.DM_CUSTOMER A
GROUP BY TX_DATE
)T2
ON T1.TX_DATE=T2.TX_DATE;

--2.�ַ����ֶ�
--������е��ַ�������û�����⴦����������Ա���¼������;��������⴦���������������ֶ��߼����ԡ�
--����¼��
V_COL := 'GENDER';
V_COL_NAME := '�Ա�';
V_COL_DES := '���Ա�������';
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
,COALESCE(T1.TX_DATE,T2.TX_DATE)
,V_COL
,V_COL_NAME
,V_COL_DES
,V_TEST_DES
,T1.ST_NUM
,T2.DM_NUM
,NVL(T1.ST_NUM,0)-nvl(T2.DM_NUM,0)
,NULL
FROM
(
SELECT TX_DATE,COUNT(*) AS ST_NUM,GENDER_CODE FROM 
MMAPST.ST_CUSTOMER 
GROUP BY TX_DATE,GENDER_CODE
) T1
FULL JOIN
(
SELECT TX_DATE,COUNT(*) AS DM_NUM,GENDER_CODE
FROM MMAPDM.DM_CUSTOMER 
GROUP BY TX_DATE,GENDER_CODE
)T2
ON T1.TX_DATE=T2.TX_DATE 
AND T1.GENDER_CODE=T2.GENDER_CODE 
;
--3.�ַ����ֶ�
--������е��ַ�������û�����⴦����������Ա���¼������;��������⴦���������������ֶ��߼����ԡ�
--����¼��
V_COL := 'EDU_CODE';
V_COL_NAME := '�����̶�';
V_COL_DES := '�������̶ȷ������';
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
,COALESCE(T1.TX_DATE,T2.TX_DATE)
,V_COL
,V_COL_NAME
,V_COL_DES
,V_TEST_DES
,T1.ST_NUM
,T2.DM_NUM
,NVL(T1.ST_NUM,0)-nvl(T2.DM_NUM,0)
,NULL
FROM
(
SELECT TX_DATE,COUNT(*) AS ST_NUM,EDU_CODE FROM 
MMAPST.ST_CUSTOMER 
GROUP BY TX_DATE,EDU_CODE
) T1
FULL JOIN
(
SELECT TX_DATE,COUNT(*) AS DM_NUM,EDU_CODE
FROM MMAPDM.DM_CUSTOMER 
GROUP BY TX_DATE,EDU_CODE
)T2
ON T1.TX_DATE=T2.TX_DATE 
AND T1.EDU_CODE=T2.EDU_CODE 
;

--4.�ַ����ֶ�
--������е��ַ�������û�����⴦����������Ա���¼������;��������⴦���������������ֶ��߼����ԡ�
--����¼��
V_COL := 'MARITALSTS_CODE';
V_COL_NAME := '����״̬';
V_COL_DES := '������״̬�������';
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
,COALESCE(T1.TX_DATE,T2.TX_DATE)
,V_COL
,V_COL_NAME
,V_COL_DES
,V_TEST_DES
,T1.ST_NUM
,T2.DM_NUM
,NVL(T1.ST_NUM,0)-nvl(T2.DM_NUM,0)
,NULL
FROM
(
SELECT TX_DATE,COUNT(*) AS ST_NUM,MARITALSTS_CODE FROM 
MMAPST.ST_CUSTOMER 
GROUP BY TX_DATE,MARITALSTS_CODE
) T1
FULL JOIN
(
SELECT TX_DATE,COUNT(*) AS DM_NUM,MARITALSTS_CODE
FROM MMAPDM.DM_CUSTOMER 
GROUP BY TX_DATE,MARITALSTS_CODE
)T2
ON T1.TX_DATE=T2.TX_DATE 
AND T1.MARITALSTS_CODE=T2.MARITALSTS_CODE 
;


--5.�ַ����ֶ�
--������е��ַ�������û�����⴦����������Ա���¼������;��������⴦���������������ֶ��߼����ԡ�
--����¼��
V_COL := 'OCCP_CODE';
V_COL_NAME := 'ְҵ';
V_COL_DES := '��ְҵ�������';
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
,COALESCE(T1.TX_DATE,T2.TX_DATE)
,V_COL
,V_COL_NAME
,V_COL_DES
,V_TEST_DES
,T1.ST_NUM
,T2.DM_NUM
,NVL(T1.ST_NUM,0)-nvl(T2.DM_NUM,0)
,NULL
FROM
(
SELECT TX_DATE,COUNT(*) AS ST_NUM,OCCP_CODE FROM 
MMAPST.ST_CUSTOMER 
GROUP BY TX_DATE,OCCP_CODE
) T1
FULL JOIN
(
SELECT TX_DATE,COUNT(*) AS DM_NUM,OCCP_CODE
FROM MMAPDM.DM_CUSTOMER 
GROUP BY TX_DATE,OCCP_CODE
)T2
ON T1.TX_DATE=T2.TX_DATE 
AND T1.OCCP_CODE=T2.OCCP_CODE 
;

--6.�ַ����ֶ�
--������е��ַ�������û�����⴦����������Ա���¼������;��������⴦���������������ֶ��߼����ԡ�
--����¼��
V_COL := 'POSITION_CODE';
V_COL_NAME := 'ְλ';
V_COL_DES := '��ְҵλ�������';
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
,COALESCE(T1.TX_DATE,T2.TX_DATE)
,V_COL
,V_COL_NAME
,V_COL_DES
,V_TEST_DES
,T1.ST_NUM
,T2.DM_NUM
,NVL(T1.ST_NUM,0)-nvl(T2.DM_NUM,0)
,NULL
FROM
(
SELECT TX_DATE,COUNT(*) AS ST_NUM,POSITION_CODE FROM 
MMAPST.ST_CUSTOMER 
GROUP BY TX_DATE,POSITION_CODE
) T1
FULL JOIN
(
SELECT TX_DATE,COUNT(*) AS DM_NUM,POSITION_CODE
FROM MMAPDM.DM_CUSTOMER 
GROUP BY TX_DATE,POSITION_CODE
)T2
ON T1.TX_DATE=T2.TX_DATE 
AND T1.POSITION_CODE=T2.POSITION_CODE 
;

--7.�ַ����ֶ�
--������е��ַ�������û�����⴦����������Ա���¼������;��������⴦���������������ֶ��߼����ԡ�
--����¼��
V_COL := 'COUNTRY_CODE';
V_COL_NAME := '����';
V_COL_DES := '�����ҷ������';
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
,COALESCE(T1.TX_DATE,T2.TX_DATE)
,V_COL
,V_COL_NAME
,V_COL_DES
,V_TEST_DES
,T1.ST_NUM
,T2.DM_NUM
,NVL(T1.ST_NUM,0)-nvl(T2.DM_NUM,0)
,NULL
FROM
(
SELECT TX_DATE,COUNT(*) AS ST_NUM,COUNTRY_CODE FROM 
MMAPST.ST_CUSTOMER 
GROUP BY TX_DATE,COUNTRY_CODE
) T1
FULL JOIN
(
SELECT TX_DATE,COUNT(*) AS DM_NUM,COUNTRY_CODE
FROM MMAPDM.DM_CUSTOMER 
GROUP BY TX_DATE,COUNTRY_CODE
)T2
ON T1.TX_DATE=T2.TX_DATE 
AND T1.COUNTRY_CODE=T2.COUNTRY_CODE 
;

--8.�ַ����ֶ�
--������е��ַ�������û�����⴦����������Ա���¼������;��������⴦���������������ֶ��߼����ԡ�
--����¼��
V_COL := 'RACE_CODE';
V_COL_NAME := '����';
V_COL_DES := '������������';
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
,COALESCE(T1.TX_DATE,T2.TX_DATE)
,V_COL
,V_COL_NAME
,V_COL_DES
,V_TEST_DES
,T1.ST_NUM
,T2.DM_NUM
,NVL(T1.ST_NUM,0)-nvl(T2.DM_NUM,0)
,NULL
FROM
(
SELECT TX_DATE,COUNT(*) AS ST_NUM,RACE_CODE FROM 
MMAPST.ST_CUSTOMER 
GROUP BY TX_DATE,RACE_CODE
) T1
FULL JOIN
(
SELECT TX_DATE,COUNT(*) AS DM_NUM,RACE_CODE
FROM MMAPDM.DM_CUSTOMER 
GROUP BY TX_DATE,RACE_CODE
)T2
ON T1.TX_DATE=T2.TX_DATE 
AND T1.RACE_CODE=T2.RACE_CODE 
;

--9.�ַ����ֶ�
--������е��ַ�������û�����⴦����������Ա���¼������;��������⴦���������������ֶ��߼����ԡ�
--����¼��
V_COL := 'CIF_LVL_CODE';
V_COL_NAME := '�ͻ�����';
V_COL_DES := '���ͻ�����������';
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
,COALESCE(T1.TX_DATE,T2.TX_DATE)
,V_COL
,V_COL_NAME
,V_COL_DES
,V_TEST_DES
,T1.ST_NUM
,T2.DM_NUM
,NVL(T1.ST_NUM,0)-nvl(T2.DM_NUM,0)
,NULL
FROM
(
SELECT TX_DATE,COUNT(*) AS ST_NUM,CIF_LVL_CODE FROM 
MMAPST.ST_CUSTOMER 
GROUP BY TX_DATE,CIF_LVL_CODE
) T1
FULL JOIN
(
SELECT TX_DATE,COUNT(*) AS DM_NUM,CIF_LVL_CODE
FROM MMAPDM.DM_CUSTOMER 
GROUP BY TX_DATE,CIF_LVL_CODE
)T2
ON T1.TX_DATE=T2.TX_DATE 
AND T1.CIF_LVL_CODE=T2.CIF_LVL_CODE 
;

--10.�ַ����ֶ�
--������е��ַ�������û�����⴦����������Ա���¼������;��������⴦���������������ֶ��߼����ԡ�
--����¼��
V_COL := 'RISK_LVL_CODE';
V_COL_NAME := '���յȼ�';
V_COL_DES := '�����յȼ��������';
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
,COALESCE(T1.TX_DATE,T2.TX_DATE)
,V_COL
,V_COL_NAME
,V_COL_DES
,V_TEST_DES
,T1.ST_NUM
,T2.DM_NUM
,NVL(T1.ST_NUM,0)-nvl(T2.DM_NUM,0)
,NULL
FROM
(
SELECT TX_DATE,COUNT(*) AS ST_NUM,RISK_LVL_CODE FROM 
MMAPST.ST_CUSTOMER 
GROUP BY TX_DATE,RISK_LVL_CODE
) T1
FULL JOIN
(
SELECT TX_DATE,COUNT(*) AS DM_NUM,RISK_LVL_CODE
FROM MMAPDM.DM_CUSTOMER 
GROUP BY TX_DATE,RISK_LVL_CODE
)T2
ON T1.TX_DATE=T2.TX_DATE 
AND T1.RISK_LVL_CODE=T2.RISK_LVL_CODE 
;

--11.�ַ����ֶ�
--������е��ַ�������û�����⴦����������Ա���¼������;��������⴦���������������ֶ��߼����ԡ�
--����¼��
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
,COALESCE(T1.TX_DATE,T2.TX_DATE)
,V_COL
,V_COL_NAME
,V_COL_DES
,V_TEST_DES
,T1.ST_NUM
,T2.DM_NUM
,NVL(T1.ST_NUM,0)-nvl(T2.DM_NUM,0)
,NULL
FROM
(
SELECT TX_DATE,COUNT(*) AS ST_NUM FROM MMAPST.ST_CUSTOMER WHERE EMP_IND='1'
GROUP BY TX_DATE
) T1
FULL JOIN
(
SELECT TX_DATE,COUNT(*) AS DM_NUM FROM MMAPDM.DM_CUSTOMER WHERE EMP_IND='1'
GROUP BY TX_DATE
)T2
ON T1.TX_DATE=T2.TX_DATE 
;


IO_ROW := SQL%ROWCOUNT ; --��¼�洢���̼�¼��
COMMIT;
SELECT SYSDATE INTO  V_END_TIMESTAMP  FROM dual;  -- ���س������н���ʱ��
IO_STATUS := 0 ;
VO_SQLERR := 'SUSSCESS';

P_TEST_WRITE_LOGS

(TRANS_NAME,IO_STATUS,IO_ROW,V_START_TIMESTAMP,V_END_TIMESTAMP,VO_SQLERR);

EXCEPTION
    WHEN OTHERS THEN
  ROLLBACK ;
  IO_STATUS := 9 ;
  VO_SQLERR := SQLCODE ||  SQLERRM  ;
  SELECT SYSDATE INTO  V_END_TIMESTAMP  FROM dual;
  P_TEST_WRITE_LOGS

(TRANS_NAME,IO_STATUS,IO_ROW,V_START_TIMESTAMP,V_END_TIMESTAMP,VO_SQLERR);



END  P_TEST_ST_DM_CUSTOMER;
/