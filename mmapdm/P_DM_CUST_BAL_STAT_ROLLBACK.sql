CREATE OR REPLACE PROCEDURE P_DM_CUST_BAL_STAT_ROLLBACK (
    TABLE_NAME IN VARCHAR2, --����
    DM_TODAY IN NUMBER,--����
    DM_YESTERDAY IN NUMBER, -- ��������"��һ��"
    FREQ IN VARCHAR2 --Ƶ��  
   ) 
   AS
       DM_SQL VARCHAR2(20000);
   BEGIN
     DM_SQL :='UPDATE MMAPDM.'||TABLE_NAME||' SET FREQ_DIFF = FREQ_DIFF - 1
        WHERE (SELECT DAYOFWEEK FROM MMAPST.MID_CALENDAR WHERE PERIOD_ID='||DM_TODAY||')=1
      ';
    EXECUTE IMMEDIATE DM_SQL;
    COMMIT;
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
    COMMIT;

   END P_DM_CUST_BAL_STAT_ROLLBACK;
/
