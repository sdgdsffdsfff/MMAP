CREATE OR REPLACE PROCEDURE "P_DM_CUST_CARD_LVL_HIS"

  AS
  VO_SQLERR  VARCHAR2(265);
  TABLE_NAME VARCHAR2(125) :='DM_CUST_CARD_LVL_HIS';  -- ����(�޸�)

  IO_ROW INTEGER;
  IO_STATUS INTEGER;

  RETURN_NUM NUMBER;                -- ����״ֵ̬
  RETURN_STATUS VARCHAR2(5000);          -- ���ص�����

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

  SELECT SYSDATE INTO V_START_TIMESTAMP FROM dual;  -- ���س������п�ʼʱ��

  SELECT TO_NUMBER(TO_CHAR((SYSDATE-1),'YYYYMMDD')) INTO DM_TODAY FROM dual;  -- ȡ"��һ��"ϵͳ����Ϊ"����",��Ϊϵͳ�����߼�ΪT+1

  P_MMAPDM_CREATE_TMP_TABLES(TABLE_NAME , RETURN_NUM , RETURN_STATUS,TMP_PRE,TMP_CUR,TMP_INS,TMP_UPD); -- ���ô�����ʱ���洢����

  --�ָ�����Ϊǰһ������״̬��Ϊ�������������������
  DELETE FROM  DM_CUST_CARD_LVL_HIS   WHERE DM_START_DT = DM_TODAY;  -- ɾ����ʼ����Ϊ"����"������

  UPDATE DM_CUST_CARD_LVL_HIS SET DM_END_DT= DM_MAX_DATE  WHERE DM_END_DT=  DM_TODAY;  -- ���½�������Ϊ"����"������Ϊ�������

  COMMIT;

  /* ȡǰһ������  */
  DM_SQL := '  INSERT INTO  '||  TMP_PRE  || '
  (
     CUSTOMER_ID      --  �ͻ���
    ,CIF_LVL_CODE     --�ͻ�����_����
    ,CIF_LVL          --�ͻ�����
    ,CIF_LVL_DATE     --�ͻ����𶨼�����
    ,CARD_LVL_CODE    --�ͻ�������_����_��ǿ�
    ,CARD_LVL         --�ͻ�������_��ǿ�
    ,CARD_DATE        --��ǿ���������
    ,ETL_DATE         --  ��������
    ,DM_START_DT      --  ��ʼ����
    ,DM_END_DT        --  ��������

  )
  SELECT
     CUSTOMER_ID      --  �ͻ���
    ,CIF_LVL_CODE     --�ͻ�����_����
    ,CIF_LVL          --�ͻ�����
    ,CIF_LVL_DATE     --�ͻ����𶨼�����
    ,CARD_LVL_CODE    --�ͻ�������_����_��ǿ�
    ,CARD_LVL         --�ͻ�������_��ǿ�
    ,CARD_DATE        --��ǿ���������
    ,ETL_DATE         --  ��������
    ,DM_START_DT      --  ��ʼ����
    ,DM_END_DT        --  ��������
  FROM MMAPDM.DM_CUST_CARD_LVL_HIS
  WHERE DM_END_DT  = '  || DM_MAX_DATE ;
  EXECUTE  IMMEDIATE DM_SQL;
  COMMIT;
 
  /*ȡ��������*/
  DM_SQL := 'INSERT INTO '|| TMP_CUR || '
  (
     CUSTOMER_ID      --  �ͻ���
    ,CIF_LVL_CODE     --�ͻ�����_����
    ,CIF_LVL          --�ͻ�����
    ,CIF_LVL_DATE     --�ͻ����𶨼�����
    ,CARD_LVL_CODE    --�ͻ�������_����_��ǿ�
    ,CARD_LVL         --�ͻ�������_��ǿ�
    ,CARD_DATE        --��ǿ���������
    ,ETL_DATE         --  ��������
    ,DM_START_DT      --  ��ʼ����
    ,DM_END_DT        --  ��������
  )
  SELECT 
  a.CUSTOMER_ID AS CUSTOMER_ID
 ,NVL(c.VALUE_NUM,0) AS CIF_LVL_CODE
 ,NVL(c.ITEM_NM,''δ֪'')  AS CIF_LVL
 ,a.TX_DATE AS CIF_LVL_DATE
 ,NVL(b.VALUE_NUM,0) AS  CARD_LVL_CODE
 ,NVL(b.ITEM_NM,''δ֪'')  AS CARD_LVL
 ,nvl(b.ISSU_DT,To_date(99991231,''yyyy-mm-dd HH24:mi:ss'')) AS CARD_DATE
 ,TO_NUMBER(TO_CHAR(SYSDATE,''YYYYMMDD'')) AS ETL_DATE
 ,'||  DM_TODAY ||  '
 ,'||  DM_MAX_DATE  ||'
 FROM MMAPST.ST_CUSTOMER a
LEFT JOIN   
 (   
SELECT CUSTOMER_ID,VALUE_NUM,ITEM_NM,ISSU_DT FROM  (
SELECT Row_number() OVER (PARTITION BY CUSTOMER_ID ORDER BY b.VALUE_NUM DESC,Issu_dt DESC) AS ROW_NUM
        ,a.CUSTOMER_ID
        ,a.CrdTyp_ID
        ,b.ITEM_NM
        ,b.VALUE_NUM
        ,a.ISSU_DT
FROM  MMAPST.ST_CUST_CARD a
LEFT JOIN                   
(
SELECT SOURCE_ITEM_ID,SOURCE_ITEM_NM,ITEM_ID,ITEM_NM,VALUE_NUM 
FROM MMAPST.ST_ITEM 
WHERE ITEM_TYP=''CARD_LVL''
) b
ON
a.CRDTYP_ID=b.SOURCE_ITEM_ID                            
)  t               
WHERE t. ROW_NUM=1           
 ) b
on a.CUSTOMER_ID=b.CUSTOMER_ID
LEFT JOIN 
(SELECT SOURCE_ITEM_ID,SOURCE_ITEM_NM,ITEM_ID,ITEM_NM,VALUE_NUM FROM MMAPST.ST_ITEM  WHERE ITEM_TYP=''CIF_LVL'') c
ON a.CIF_LVL_CODE=c.SOURCE_ITEM_ID
WHERE a.CUSTOMER_ID in (''8700167982'')';

  EXECUTE  IMMEDIATE DM_SQL;
  COMMIT;

  /*cur-pre =  �������ݣ������޸ĺ�*/
  DM_SQL := 'INSERT INTO '|| TMP_INS || '
  (
    CUSTOMER_ID      --  �ͻ���
    ,CIF_LVL_CODE     --�ͻ�����_����
    ,CIF_LVL          --�ͻ�����
    ,CIF_LVL_DATE     --�ͻ����𶨼�����
    ,CARD_LVL_CODE    --�ͻ�������_����_��ǿ�
    ,CARD_LVL         --�ͻ�������_��ǿ�
    ,CARD_DATE        --��ǿ���������
    ,ETL_DATE         --  ��������
    ,DM_START_DT      --  ��ʼ����
    ,DM_END_DT        --  ��������
  )
  
  SELECT 
     a.CUSTOMER_ID      --  �ͻ���
    ,a.CIF_LVL_CODE     --�ͻ�����_����
    ,a.CIF_LVL          --�ͻ�����
    ,CASE WHEN a.CIF_LVL_CODE=c.CIF_LVL_CODE then  a.CIF_LVL_DATE 
          ELSE TO_DATE('||DM_TODAY||',''yyyy-mm-dd HH24:mi:ss'') END AS CIF_LVL_DATE --�ͻ����𶨼�����
    ,a.CARD_LVL_CODE    --�ͻ�������_����_��ǿ�
    ,a.CARD_LVL         --�ͻ�������_��ǿ�
    ,a.CARD_DATE        --��ǿ���������
    ,a.ETL_DATE         --  ��������
    ,a.DM_START_DT      --  ��ʼ����
    ,a.DM_END_DT        --  ��������
  
 FROM
 ( 
    SELECT
     CUSTOMER_ID      --  �ͻ���
    ,CIF_LVL_CODE     --�ͻ�����_����
    ,CIF_LVL          --�ͻ�����
    ,CIF_LVL_DATE     --�ͻ����𶨼�����
    ,CARD_LVL_CODE    --�ͻ�������_����_��ǿ�
    ,CARD_LVL         --�ͻ�������_��ǿ�
    ,CARD_DATE        --��ǿ���������
    ,ETL_DATE         --  ��������
    ,DM_START_DT      --  ��ʼ����
    ,DM_END_DT        --  ��������
  FROM '|| TMP_CUR ||  '  a
  WHERE
  NOT  EXISTS
  (
    SELECT
     CUSTOMER_ID      --  �ͻ���
    ,CIF_LVL_CODE     --�ͻ�����_����
    ,CIF_LVL          --�ͻ�����
    ,CIF_LVL_DATE     --�ͻ����𶨼�����
    ,CARD_LVL_CODE    --�ͻ�������_����_��ǿ�
    ,CARD_LVL         --�ͻ�������_��ǿ�
    ,CARD_DATE        --��ǿ���������
    FROM '|| TMP_PRE ||  '  b
    WHERE
         TRIM(a.CUSTOMER_ID)= TRIM(b.CUSTOMER_ID)
    AND  TRIM(a.CIF_LVL_CODE)= TRIM(b.CIF_LVL_CODE)
    AND  TRIM(a.CIF_LVL)= TRIM(b.CIF_LVL)
    AND  TRIM(a.CIF_LVL_DATE)=TRIM(b.CIF_LVL_DATE)
    AND  TRIM(nvl(a.CARD_LVL_CODE,0))= TRIM(nvl(b.CARD_LVL_CODE,0))
    AND  TRIM(nvl(a.CARD_LVL,''δ֪''))=nvl( b.CARD_LVL,''δ֪'')
    AND  TRIM(nvl(a.CARD_DATE,To_date(99991231,''yyyy-mm-dd HH24:mi:ss'')))=TRIM(nvl(b.CARD_DATE,To_date(99991231,''yyyy-mm-dd HH24:mi:ss'')))
  )
  
  ) a
 LEFT JOIN '|| TMP_PRE ||  '  c
 ON TRIM(a.CUSTOMER_ID)=TRIM(c.CUSTOMER_ID)';

  EXECUTE  IMMEDIATE DM_SQL;
  COMMIT;

   /*  pre-cur  = ɾ���ݣ������޸�ǰ�� */
  DM_SQL := 'INSERT INTO '|| TMP_UPD || '
  (
     CUSTOMER_ID      --  �ͻ���
    ,CIF_LVL_CODE     --�ͻ�����_����
    ,CIF_LVL          --�ͻ�����
    ,CIF_LVL_DATE     --�ͻ����𶨼�����
    ,CARD_LVL_CODE    --�ͻ�������_����_��ǿ�
    ,CARD_LVL         --�ͻ�������_��ǿ�
    ,CARD_DATE        --��ǿ���������
    ,ETL_DATE         --  ��������
    ,DM_START_DT      --  ��ʼ����
    ,DM_END_DT        --  ��������
  )
  SELECT
     CUSTOMER_ID      --  �ͻ���
    ,CIF_LVL_CODE     --�ͻ�����_����
    ,CIF_LVL          --�ͻ�����
    ,CIF_LVL_DATE     --�ͻ����𶨼�����
    ,CARD_LVL_CODE    --�ͻ�������_����_��ǿ�
    ,CARD_LVL         --�ͻ�������_��ǿ�
    ,CARD_DATE        --��ǿ���������
    ,ETL_DATE         --  ��������
    ,DM_START_DT      --  ��ʼ����
    ,DM_END_DT        --  ��������
  FROM '|| TMP_PRE ||' a
  WHERE
  NOT  EXISTS
  (
    SELECT
     CUSTOMER_ID      --  �ͻ���
    ,CIF_LVL_CODE     --�ͻ�����_����
    ,CIF_LVL          --�ͻ�����
    ,CIF_LVL_DATE     --�ͻ����𶨼�����
    ,CARD_LVL_CODE    --�ͻ�������_����_��ǿ�
    ,CARD_LVL         --�ͻ�������_��ǿ�
    ,CARD_DATE        --��ǿ���������
    ,ETL_DATE         --  ��������
    ,DM_START_DT      --  ��ʼ����
    ,DM_END_DT        --  ��������
    FROM '|| TMP_CUR ||  '  b
    WHERE
         TRIM(a.CUSTOMER_ID)= TRIM(b.CUSTOMER_ID)
    AND  TRIM(a.CIF_LVL_CODE)= TRIM(b.CIF_LVL_CODE)
    AND  TRIM(a.CIF_LVL)= TRIM(b.CIF_LVL)
    AND  TRIM(a.CIF_LVL_DATE)=TRIM(b.CIF_LVL_DATE)
    AND  TRIM(nvl(a.CARD_LVL_CODE,0))= TRIM(nvl(b.CARD_LVL_CODE,0))
    AND  TRIM(nvl(a.CARD_LVL,''δ֪''))=nvl( b.CARD_LVL,''δ֪'')
    AND  TRIM(nvl(a.CARD_DATE,To_date(99991231,''yyyy-mm-dd HH24:mi:ss'')))=TRIM(nvl(b.CARD_DATE,To_date(99991231,''yyyy-mm-dd HH24:mi:ss'')))
  )';
  EXECUTE  IMMEDIATE DM_SQL;
  COMMIT;

  /* ������ʷ���������ݵĽ�ֹʱ��Ϊ����ʱ��(ɾ��-�����������) */
  DM_SQL := 'DELETE FROM MMAPDM.DM_CUST_CARD_LVL_HIS a
  WHERE
  EXISTS(
  SELECT
    CUSTOMER_ID      --  �ͻ���
    ,CIF_LVL_CODE     --�ͻ�����_����
    ,CIF_LVL          --�ͻ�����
    ,CIF_LVL_DATE     --�ͻ����𶨼�����
    ,CARD_LVL_CODE    --�ͻ�������_����_��ǿ�
    ,CARD_LVL         --�ͻ�������_��ǿ�
    ,CARD_DATE        --��ǿ���������
    ,ETL_DATE         --  ��������
    ,DM_START_DT      --  ��ʼ����
    ,DM_END_DT        --  ��������
  FROM '|| TMP_UPD ||  ' b
  WHERE
         a.CUSTOMER_ID= b.CUSTOMER_ID
    AND  a.CIF_LVL_CODE= b.CIF_LVL_CODE
    AND  a.CIF_LVL= b.CIF_LVL
    AND  a.CIF_LVL_DATE=  b.CIF_LVL_DATE
    AND  a.CARD_LVL_CODE= b.CARD_LVL_CODE
    AND  a.CARD_LVL= b.CARD_LVL
    AND  a.CARD_DATE= b.CARD_DATE
  )';
  EXECUTE  IMMEDIATE DM_SQL;


  DM_SQL := 'INSERT INTO MMAPDM.DM_CUST_CARD_LVL_HIS
  (
   CUSTOMER_ID      --  �ͻ���
    ,CIF_LVL_CODE     --�ͻ�����_����
    ,CIF_LVL          --�ͻ�����
    ,CIF_LVL_DATE     --�ͻ����𶨼�����
    ,CARD_LVL_CODE    --�ͻ�������_����_��ǿ�
    ,CARD_LVL         --�ͻ�������_��ǿ�
    ,CARD_DATE        --��ǿ���������
    ,ETL_DATE         --  ��������
    ,DM_START_DT      --  ��ʼ����
    ,DM_END_DT        --  ��������
  )
  SELECT
   CUSTOMER_ID      --  �ͻ���
    ,CIF_LVL_CODE     --�ͻ�����_����
    ,CIF_LVL          --�ͻ�����
    ,CIF_LVL_DATE     --�ͻ����𶨼�����
    ,CARD_LVL_CODE    --�ͻ�������_����_��ǿ�
    ,CARD_LVL         --�ͻ�������_��ǿ�
    ,CARD_DATE        --��ǿ���������
    ,ETL_DATE         --  ��������
    ,DM_START_DT      --  ��ʼ����
    ,'|| DM_TODAY ||'
  FROM '|| TMP_UPD ||  '';
  EXECUTE  IMMEDIATE DM_SQL;


  /* ������������  */
  DM_SQL := 'INSERT INTO  MMAPDM.DM_CUST_CARD_LVL_HIS
  (
    CUSTOMER_ID      --  �ͻ���
    ,CIF_LVL_CODE     --�ͻ�����_����
    ,CIF_LVL          --�ͻ�����
    ,CIF_LVL_DATE     --�ͻ����𶨼�����
    ,CARD_LVL_CODE    --�ͻ�������_����_��ǿ�
    ,CARD_LVL         --�ͻ�������_��ǿ�
    ,CARD_DATE        --��ǿ���������
    ,ETL_DATE         --  ��������
    ,DM_START_DT      --  ��ʼ����
    ,DM_END_DT        --  ��������
  )
  SELECT
   CUSTOMER_ID      --  �ͻ���
    ,CIF_LVL_CODE     --�ͻ�����_����
    ,CIF_LVL          --�ͻ�����
    ,CIF_LVL_DATE     --�ͻ����𶨼�����
    ,CARD_LVL_CODE    --�ͻ�������_����_��ǿ�
    ,CARD_LVL         --�ͻ�������_��ǿ�
    ,CARD_DATE        --��ǿ���������
    ,ETL_DATE         --  ��������
    ,DM_START_DT      --  ��ʼ����
    ,DM_END_DT        --  ��������
  FROM '|| TMP_INS ||  '';
  EXECUTE  IMMEDIATE DM_SQL;

  IO_ROW := SQL%ROWCOUNT ;

  --P_MMAPDM_DROP_TMP_TABLES(TABLE_NAME,RETURN_NUM,RETURN_STATUS);

  SELECT SYSDATE INTO  V_END_TIMESTAMP  FROM dual;  -- ���س������н���ʱ��


  IO_STATUS := 0 ;
  VO_SQLERR := 'SUSSCESS';
  P_MMAPDM_WRITE_LOGS(TABLE_NAME,IO_STATUS,IO_ROW,V_START_TIMESTAMP,V_END_TIMESTAMP,VO_SQLERR);
  COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
  ROLLBACK ;
  IO_STATUS := 9 ;
  VO_SQLERR := SQLCODE ||  SQLERRM  ;
  SELECT SYSDATE INTO  V_END_TIMESTAMP  FROM dual;
  P_MMAPDM_WRITE_LOGS(TABLE_NAME,IO_STATUS,IO_ROW,V_START_TIMESTAMP,V_END_TIMESTAMP,VO_SQLERR);
END  P_DM_CUST_CARD_LVL_HIS;
/