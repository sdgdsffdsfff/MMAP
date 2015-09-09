CREATE OR REPLACE PROCEDURE P_DM_CUST_BAL_STAT_FREQ
(TABLE_NAME IN VARCHAR2, --����
  FREQ IN VARCHAR2, --Ƶ��
  FREQ_VALUE IN VARCHAR2, --Ƶ��ֵ
  DAY_OF_FREQ IN VARCHAR2, --Ƶ���еĵڼ���
  FREQ_HIS IN NUMBER, --Ƶ��������ʷ����
  DM_TODAY IN NUMBER,        -- ��������"����"
  DM_YESTERDAY IN NUMBER, -- ��������"��һ��"
  IO_ROW OUT INTEGER,
  IO_STATUS OUT INTEGER
)

AS

  PROCEDURE_NAME VARCHAR2(125) :='P_'||TABLE_NAME;  -- �洢������(�޸�)

  
  IO_SQLERR VARCHAR2(2000);

  DM_SQL VARCHAR2(30000); -- the variable to loading the SQL statment


  ST_ROW INTEGER;  --Դ��������


  V_START_TIMESTAMP TIMESTAMP;    --the start time of procedures
  V_END_TIMESTAMP   TIMESTAMP;    --the end time of procedures



BEGIN

  IO_ROW := 0;  --��������

  SELECT SYSDATE INTO V_START_TIMESTAMP FROM dual;    -- ���س������п�ʼʱ��

  --�ָ�'��һ��'���ݡ�
  --��ѯDM������Ƿ���"����"���ݣ��Ƿ�Ϊ���ܡ�
  DM_SQL := ' SELECT COUNT(1) FROM MMAPDM.'||TABLE_NAME||' WHERE PERIOD_ID='||DM_TODAY;
  EXECUTE  IMMEDIATE DM_SQL  INTO ST_ROW;
  IF ST_ROW>0
    --���Ϊ�������ݣ������ݻָ�Ϊ'ǰһ��'����״̬
  THEN
    --1.ɾ��'����'����
    DM_SQL :='DELETE FROM MMAPDM.'||TABLE_NAME||' WHERE PERIOD_ID='||DM_TODAY;
    EXECUTE IMMEDIATE DM_SQL;
    IO_ROW := IO_ROW+SQL%ROWCOUNT ;
    COMMIT;
    --2.�ָ�Ƶ�Ȳ�
    DM_SQL :='UPDATE MMAPDM.'||TABLE_NAME||' SET FREQ_DIFF = FREQ_DIFF - 1
        WHERE (SELECT '||DAY_OF_FREQ||' FROM MMAPST.MID_CALENDAR WHERE PERIOD_ID='||DM_TODAY||')=1
      ';
    EXECUTE IMMEDIATE DM_SQL;
    IO_ROW := IO_ROW+SQL%ROWCOUNT ;
    COMMIT;
    --3.������������
    DM_SQL:= 'INSERT INTO MMAPDM.'||TABLE_NAME||'
    (
         ETL_DATE               --��������(YYYYMMDD)
        ,TX_DATE                --��������(YYYYMMDD)
        ,PERIOD_ID              --����(YYYYMMDD)
        ,FREQ                   --Ƶ�ȣ�D\W\M\Q\Y��
        ,YEAR                   --���(YYYY)
        ,FREQ_VALUE             --Ƶ��ֵ(1\2\3\4)
        ,FREQ_DIFF              --Ƶ�Ȳ�(��������ڵļ��Ȳ�ֵ)
        ,CUSTOMER_ID            --�ͻ���
        ,PROD_TYPE              --��Ʒ���ࣨ���ڡ����ڡ������ʲ��ܶ�����+��ң��ȣ�
        ,CUST_BAL_LC            --���
        ,CUST_BAL_CWS_LC        --���_ͬ��
        ,CUST_BAL_SQT_LC        --���_����
        ,CUST_BAL_MAX_LC        --���_���ֵ
        ,CUST_BAL_MAX_DATE_LC   --���_���ֵ_����
        ,CUST_BAL_MIN_LC        --���_��Сֵ
        ,CUST_BAL_MIN_DATE_LC   --���_��Сֵ_����
        ,CUST_BAL_AVG_LC        --��ƽ�����
        ,CUST_BAL_AVG_CWS_LC    --��ƽ�����_ͬ��
        ,CUST_BAL_AVG_SQT_LC    --��ƽ�����_����
        ,CUST_BAL_FC            --������
        ,CUST_BAL_CWS_FC        --������_ͬ��
        ,CUST_BAL_SQT_FC        --������_����
        ,CUST_BAL_MAX_FC        --������_���ֵ
        ,CUST_BAL_MAX_DATE_FC   --������_���ֵ_����
        ,CUST_BAL_MIN_FC        --������_��Сֵ
        ,CUST_BAL_MIN_DATE_FC   --������_��Сֵ_����
        ,CUST_BAL_AVG_FC        --�����ƽ�����
        ,CUST_BAL_AVG_CWS_FC    --�����ƽ�����_ͬ��
        ,CUST_BAL_AVG_SQT_FC    --�����ƽ�����_����
        ,CUST_BAL               --�ϼ����
        ,CUST_BAL_CWS           --�ϼ����_ͬ��
        ,CUST_BAL_SQT           --�ϼ����_����
        ,CUST_BAL_MAX           --�ϼ����_���ֵ
        ,CUST_BAL_MAX_DATE      --�ϼ����_���ֵ_����
        ,CUST_BAL_MIN           --�ϼ����_��Сֵ
        ,CUST_BAL_MIN_DATE      --�ϼ����_��Сֵ_����
        ,CUST_BAL_AVG           --�ϼ���ƽ�����
        ,CUST_BAL_AVG_CWS       --�ϼ���ƽ�����_ͬ��
        ,CUST_BAL_AVG_SQT       --�ϼ���ƽ�����_����
    )
    SELECT
         ETL_DATE
        ,TX_DATE
        ,PERIOD_ID
        ,FREQ
        ,YEAR
        ,FREQ_VALUE
        ,FREQ_DIFF
        ,CUSTOMER_ID
        ,PROD_TYPE
        ,CUST_BAL_LC
        ,CUST_BAL_CWS_LC
        ,CUST_BAL_SQT_LC
        ,CUST_BAL_MAX_LC
        ,CUST_BAL_MAX_DATE_LC
        ,CUST_BAL_MIN_LC
        ,CUST_BAL_MIN_DATE_LC
        ,CUST_BAL_AVG_LC
        ,CUST_BAL_AVG_CWS_LC
        ,CUST_BAL_AVG_SQT_LC
        ,CUST_BAL_FC
        ,CUST_BAL_CWS_FC
        ,CUST_BAL_SQT_FC
        ,CUST_BAL_MAX_FC
        ,CUST_BAL_MAX_DATE_FC
        ,CUST_BAL_MIN_FC
        ,CUST_BAL_MIN_DATE_FC
        ,CUST_BAL_AVG_FC
        ,CUST_BAL_AVG_CWS_FC
        ,CUST_BAL_AVG_SQT_FC
        ,CUST_BAL
        ,CUST_BAL_CWS
        ,CUST_BAL_SQT
        ,CUST_BAL_MAX
        ,CUST_BAL_MAX_DATE
        ,CUST_BAL_MIN
        ,CUST_BAL_MIN_DATE
        ,CUST_BAL_AVG
        ,CUST_BAL_AVG_CWS
        ,CUST_BAL_AVG_SQT
    FROM  MMAPDM.DM_CUST_BAL_STAT_PRE
    WHERE FREQ='''||FREQ||'''
    AND PERIOD_ID='||DM_YESTERDAY
    ;
    EXECUTE  IMMEDIATE DM_SQL;
    IO_ROW := IO_ROW+SQL%ROWCOUNT ;
    COMMIT;
    END IF;

    --����"����"����
    --��ѯDM������Ƿ���"��һ��"���ݣ��Ƿ�Ϊ���ܡ�
    DM_SQL := ' SELECT COUNT(1) FROM MMAPDM.'||TABLE_NAME||' WHERE PERIOD_ID='||DM_YESTERDAY;
    EXECUTE  IMMEDIATE DM_SQL  INTO ST_ROW;
    IF ST_ROW>0
    THEN
      DM_SQL :='DELETE FROM MMAPDM.DM_CUST_BAL_STAT_PRE WHERE FREQ='''||FREQ||'''' ;   -- ɾ�����ݱ�����Ƶ������
      EXECUTE IMMEDIATE DM_SQL;
      COMMIT;
      DM_SQL :='INSERT INTO MMAPDM.DM_CUST_BAL_STAT_PRE SELECT * FROM MMAPDM.'||TABLE_NAME||' WHERE PERIOD_ID='||DM_YESTERDAY ;   -- ���ݱ�����������
      EXECUTE IMMEDIATE DM_SQL;
      IO_ROW :=IO_ROW+ SQL%ROWCOUNT ;
      COMMIT;
    END IF;

    --1.����Ƶ�Ȳ�
    DM_SQL :='UPDATE MMAPDM.'||TABLE_NAME||' SET FREQ_DIFF = FREQ_DIFF + 1
        WHERE (SELECT '||DAY_OF_FREQ||' FROM MMAPST.MID_CALENDAR WHERE PERIOD_ID='||DM_TODAY||')=1
      ';
    EXECUTE IMMEDIATE DM_SQL;
    IO_ROW := IO_ROW+SQL%ROWCOUNT ;
    COMMIT;

    --2. ������ʱ��

    -----ȡǰһ������-----
    SELECT COUNT(1) INTO ST_ROW FROM USER_TABLES WHERE TABLE_NAME = 'TMP_CUST_BAL_CAL';
    IF ST_ROW >0
    THEN
      DM_SQL := 'DROP TABLE MMAPDM.TMP_CUST_BAL_CAL PURGE';
    EXECUTE  IMMEDIATE DM_SQL;
    END IF;

    DM_SQL:= 'CREATE TABLE MMAPDM.TMP_CUST_BAL_CAL AS
      SELECT
         A.ETL_DATE
        ,A.TX_DATE
        ,A.PERIOD_ID
        ,B.YEAR
        ,B.'||DAY_OF_FREQ||'
        ,B.'||FREQ_VALUE||' AS FREQ_VALUE
        ,'''||FREQ||'''  AS FREQ
        ,0 AS FREQ_DIFF
        ,A.CUSTOMER_ID
        ,A.PROD_TYPE AS PROD_TYPE
        ,A.CUST_BAL_LC AS CUST_BAL_LC
        --,C.CUST_BAL_LC AS CUST_BAL_CWS_LC
        ,0 AS CUST_BAL_CWS_LC
        ,D.CUST_BAL_LC AS CUST_BAL_SQT_LC
        ,CASE WHEN E.CUSTOMER_ID IS NULL  THEN A.CUST_BAL_LC
          ELSE GREATEST(NVL(A.CUST_BAL_LC,0),NVL(E.CUST_BAL_MAX_LC,0))
         END AS CUST_BAL_MAX_LC
        ,CASE WHEN E.CUSTOMER_ID IS NULL  THEN A.PERIOD_ID
          ELSE (CASE WHEN NVL(A.CUST_BAL_LC,0) - NVL(E.CUST_BAL_MAX_LC,0) > 0
            THEN a.PERIOD_ID ELSE e.CUST_BAL_MAX_DATE_LC
                END)
         END AS CUST_BAL_MAX_DATE_LC
        ,CASE WHEN E.CUSTOMER_ID IS NULL  THEN A.CUST_BAL_LC
          ELSE LEAST(NVL(A.CUST_BAL_LC,0),NVL(E.CUST_BAL_MIN_LC,0))
         END AS CUST_BAL_MIN_LC
        ,CASE WHEN E.CUSTOMER_ID IS NULL  THEN A.PERIOD_ID
          ELSE (CASE WHEN NVL(A.CUST_BAL_LC,0) - NVL(E.CUST_BAL_MIN_LC,0) < 0 THEN a.PERIOD_ID ELSE e.CUST_BAL_MIN_DATE_LC END)
         END AS CUST_BAL_MIN_DATE_LC
        ,CASE WHEN E.CUSTOMER_ID IS NULL THEN A.CUST_BAL_LC
          ELSE ((NVL(E.CUST_BAL_AVG_LC,0) * (B.'||DAY_OF_FREQ||'-1) + NVL(a.CUST_BAL_LC,0)) / b.'||DAY_OF_FREQ||')
         END AS CUST_BAL_AVG_LC
        --,C.CUST_BAL_AVG_LC        AS CUST_BAL_AVG_CWS_LC
        ,0        AS CUST_BAL_AVG_CWS_LC
        ,D.CUST_BAL_AVG_LC        AS CUST_BAL_AVG_SQT_LC
        ,A.CUST_BAL_FC            AS CUST_BAL_FC
        --,C.CUST_BAL_FC           AS CUST_BAL_CWS_FC
        ,0           AS CUST_BAL_CWS_FC
        ,D.CUST_BAL_FC           AS CUST_BAL_SQT_FC
        ,CASE WHEN E.CUSTOMER_ID IS NULL  THEN A.CUST_BAL_FC
          ELSE GREATEST(NVL(A.CUST_BAL_FC,0),NVL(E.CUST_BAL_MAX_FC,0))
         END AS CUST_BAL_MAX_FC
        ,CASE WHEN E.CUSTOMER_ID IS NULL  THEN A.PERIOD_ID
          ELSE (CASE WHEN NVL(A.CUST_BAL_FC,0) - NVL(e.CUST_BAL_MAX_FC,0) > 0 THEN a.PERIOD_ID ELSE e.CUST_BAL_MAX_DATE_FC END)
         END AS CUST_BAL_MAX_DATE_FC
        ,CASE WHEN E.CUSTOMER_ID IS NULL  THEN A.CUST_BAL_FC
          ELSE LEAST(NVL(A.CUST_BAL_FC,0),NVL(E.CUST_BAL_MIN_FC,0))
         END AS CUST_BAL_MIN_FC
        ,CASE WHEN E.CUSTOMER_ID IS NULL  THEN A.PERIOD_ID
          ELSE (CASE WHEN NVL(A.CUST_BAL_FC,0) - NVL(e.CUST_BAL_MIN_FC,0) < 0 THEN a.PERIOD_ID ELSE e.CUST_BAL_MIN_DATE_FC END)
         END AS CUST_BAL_MIN_DATE_FC
        ,CASE WHEN E.CUSTOMER_ID IS NULL  THEN A.CUST_BAL_FC
          ELSE ((NVL(E.CUST_BAL_AVG_FC,0) * (B.'||DAY_OF_FREQ||'-1) + NVL(a.CUST_BAL_FC,0)) / b.'||DAY_OF_FREQ||')
         END AS CUST_BAL_AVG_FC
        --,C.CUST_BAL_AVG_FC    AS CUST_BAL_AVG_CWS_FC
        ,0    AS CUST_BAL_AVG_CWS_FC
        ,D.CUST_BAL_AVG_FC    AS  CUST_BAL_AVG_SQT_FC
        ,A.CUST_BAL                AS CUST_BAL
        --,C.CUST_BAL                AS CUST_BAL_CWS
        ,0                AS CUST_BAL_CWS
        ,D.CUST_BAL                AS CUST_BAL_SQT
        ,CASE WHEN E.CUSTOMER_ID IS NULL  THEN A.CUST_BAL
          ELSE GREATEST(NVL(A.CUST_BAL,0),NVL(E.CUST_BAL_MAX,0))
         END AS CUST_BAL_MAX
        ,CASE WHEN E.CUSTOMER_ID IS NULL  THEN A.PERIOD_ID
          ELSE (CASE WHEN NVL(A.CUST_BAL,0) - NVL(E.CUST_BAL_MAX,0) > 0 THEN a.PERIOD_ID ELSE e.CUST_BAL_MAX_DATE END)
         END AS CUST_BAL_MAX_DATE
        ,CASE WHEN E.CUSTOMER_ID IS NULL  THEN A.CUST_BAL
          ELSE LEAST(NVL(A.CUST_BAL,0),NVL(E.CUST_BAL_MIN,0))
         END AS CUST_BAL_MIN
        ,CASE WHEN E.CUSTOMER_ID IS NULL  THEN A.PERIOD_ID
          ELSE (CASE WHEN NVL(A.CUST_BAL,0) - NVL(E.CUST_BAL_MIN,0) < 0 THEN a.PERIOD_ID ELSE e.CUST_BAL_MIN_DATE END)
         END AS CUST_BAL_MIN_DATE
        ,CASE WHEN E.CUSTOMER_ID IS NULL THEN A.CUST_BAL
          ELSE ((NVL(E.CUST_BAL_AVG,0) * (B.'||DAY_OF_FREQ||'-1) + NVL(a.CUST_BAL,0)) / b.'||DAY_OF_FREQ||')
         END AS CUST_BAL_AVG
        --,C.CUST_BAL_AVG       AS CUST_BAL_AVG_CWS
        ,0       AS CUST_BAL_AVG_CWS
        ,D.CUST_BAL_AVG       AS CUST_BAL_AVG_SQT     --�ϼ���ƽ�����_����
      FROM MMAPDM.TMP_CUST_BAL_T A
      LEFT JOIN  MMAPST.MID_CALENDAR B
      ON A.PERIOD_ID = B.PERIOD_ID
      /*
      --ͬ��c���ݲ�����ͬ������
      LEFT JOIN  MMAPDM.'||TABLE_NAME||' C
      ON B.YEAR - 1 = C.YEAR
      AND B.'||FREQ_VALUE||' = C.FREQ_VALUE
      AND A.CUSTOMER_ID = C.CUSTOMER_ID
      AND C.PROD_TYPE = A.PROD_TYPE
      */
      --����d��ȡ��һͳ��������
      LEFT JOIN MMAPDM.'||TABLE_NAME||' D
      ON D.FREQ_DIFF = 1
      AND A.CUSTOMER_ID = D.CUSTOMER_ID
      AND D.PROD_TYPE = A.PROD_TYPE
      --��ֵe��ȡ��ͳ����������
      LEFT JOIN MMAPDM.'||TABLE_NAME||' E
      ON B.YEAR = E.YEAR
      AND B.'||FREQ_VALUE||' = E.FREQ_VALUE
      AND A.CUSTOMER_ID = E.CUSTOMER_ID
      AND E.PROD_TYPE = A.PROD_TYPE
      ';
    EXECUTE IMMEDIATE DM_SQL;
    COMMIT;

    --ɾ����ͳ�����ھ����ݣ���������ʷ����
    DM_SQL:= 'DELETE FROM MMAPDM.'||TABLE_NAME||' WHERE FREQ_DIFF =0 OR FREQ_DIFF>='||FREQ_HIS;
    EXECUTE IMMEDIATE DM_SQL;
    IO_ROW := IO_ROW+SQL%ROWCOUNT ;
    COMMIT;
    --���뵱������

    DM_SQL:= 'INSERT INTO MMAPDM.'||TABLE_NAME||'
    (
         ETL_DATE
        ,TX_DATE
        ,PERIOD_ID
        ,FREQ
        ,YEAR
        ,FREQ_VALUE
        ,FREQ_DIFF
        ,CUSTOMER_ID
        ,PROD_TYPE
        ,CUST_BAL_LC
        ,CUST_BAL_CWS_LC
        ,CUST_BAL_SQT_LC
        ,CUST_BAL_MAX_LC
        ,CUST_BAL_MAX_DATE_LC
        ,CUST_BAL_MIN_LC
        ,CUST_BAL_MIN_DATE_LC
        ,CUST_BAL_AVG_LC
        ,CUST_BAL_AVG_CWS_LC
        ,CUST_BAL_AVG_SQT_LC
        ,CUST_BAL_FC
        ,CUST_BAL_CWS_FC
        ,CUST_BAL_SQT_FC
        ,CUST_BAL_MAX_FC
        ,CUST_BAL_MAX_DATE_FC
        ,CUST_BAL_MIN_FC
        ,CUST_BAL_MIN_DATE_FC
        ,CUST_BAL_AVG_FC
        ,CUST_BAL_AVG_CWS_FC
        ,CUST_BAL_AVG_SQT_FC
        ,CUST_BAL
        ,CUST_BAL_CWS
        ,CUST_BAL_SQT
        ,CUST_BAL_MAX
        ,CUST_BAL_MAX_DATE
        ,CUST_BAL_MIN
        ,CUST_BAL_MIN_DATE
        ,CUST_BAL_AVG
        ,CUST_BAL_AVG_CWS
        ,CUST_BAL_AVG_SQT
    )
    SELECT
         ETL_DATE
        ,TX_DATE
        ,PERIOD_ID
        ,FREQ
        ,YEAR
        ,FREQ_VALUE
        ,0 AS FREQ_DIFF
        ,CUSTOMER_ID
        ,PROD_TYPE
        ,CUST_BAL_LC
        ,CUST_BAL_CWS_LC
        ,CUST_BAL_SQT_LC
        ,CUST_BAL_MAX_LC
        ,CUST_BAL_MAX_DATE_LC
        ,CUST_BAL_MIN_LC
        ,CUST_BAL_MIN_DATE_LC
        ,CUST_BAL_AVG_LC
        ,CUST_BAL_AVG_CWS_LC
        ,CUST_BAL_AVG_SQT_LC
        ,CUST_BAL_FC
        ,CUST_BAL_CWS_FC
        ,CUST_BAL_SQT_FC
        ,CUST_BAL_MAX_FC
        ,CUST_BAL_MAX_DATE_FC
        ,CUST_BAL_MIN_FC
        ,CUST_BAL_MIN_DATE_FC
        ,CUST_BAL_AVG_FC
        ,CUST_BAL_AVG_CWS_FC
        ,CUST_BAL_AVG_SQT_FC
        ,CUST_BAL
        ,CUST_BAL_CWS
        ,CUST_BAL_SQT
        ,CUST_BAL_MAX
        ,CUST_BAL_MAX_DATE
        ,CUST_BAL_MIN
        ,CUST_BAL_MIN_DATE
        ,CUST_BAL_AVG
        ,CUST_BAL_AVG_CWS
        ,CUST_BAL_AVG_SQT
    FROM  MMAPDM.TMP_CUST_BAL_CAL
    ';
    EXECUTE  IMMEDIATE DM_SQL;
    IO_ROW := IO_ROW+SQL%ROWCOUNT ;

    /*
        д����־
    */

    SELECT SYSDATE INTO V_END_TIMESTAMP   FROM dual;    -- ���س������н���ʱ��
    IO_STATUS := 0 ;
    IO_SQLERR := 'SUSSCESS';
    P_MMAPDM_WRITE_LOGS(PROCEDURE_NAME,IO_STATUS,IO_ROW,V_START_TIMESTAMP,V_END_TIMESTAMP,IO_SQLERR);
    COMMIT;

    EXCEPTION
      WHEN OTHERS THEN
    ROLLBACK ;
    IO_STATUS := 9 ;
    P_DM_CUST_BAL_STAT_ROLLBACK(TABLE_NAME,DM_TODAY,DM_YESTERDAY,FREQ);
    IO_SQLERR := SQLCODE ||  SQLERRM  ;
    SELECT SYSDATE INTO  V_END_TIMESTAMP  FROM dual;
    P_MMAPDM_WRITE_LOGS(PROCEDURE_NAME,IO_STATUS,IO_ROW,V_START_TIMESTAMP,V_END_TIMESTAMP,IO_SQLERR);
END P_DM_CUST_BAL_STAT_FREQ;
/
