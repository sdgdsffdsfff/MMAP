CREATE OR REPLACE PROCEDURE "P_DM_CUST_CARD_LVL_HIS"

  AS
  VO_SQLERR  VARCHAR2(265);
  TABLE_NAME VARCHAR2(125) :='DM_CUST_CARD_LVL_HIS';  -- 表名(修改)

  IO_ROW INTEGER;
  IO_STATUS INTEGER;

  RETURN_NUM NUMBER;                -- 返回状态值
  RETURN_STATUS VARCHAR2(5000);          -- 返回的描述

  V_START_TIMESTAMP TIMESTAMP;  -- 加载开始时间
  V_END_TIMESTAMP    TIMESTAMP;  -- 加载结束时间

  DM_SQL VARCHAR2(20000);  -- 存放SQL语句

  DM_TODAY NUMBER;    -- 数据日期"当日"
  DM_MAX_DATE NUMBER := 99991231;  -- 最大日期

  TMP_PRE  VARCHAR(35);  -- 前一天数据临时表
  TMP_CUR  VARCHAR(35);  -- 当日数据临时表
  TMP_INS  VARCHAR(35);  -- 新插入数据临时表
  TMP_UPD  VARCHAR(35);  -- 更新数据临时表

  BEGIN

  SELECT SYSDATE INTO V_START_TIMESTAMP FROM dual;  -- 加载程序运行开始时间

  SELECT TO_NUMBER(TO_CHAR((SYSDATE-1),'YYYYMMDD')) INTO DM_TODAY FROM dual;  -- 取"上一日"系统日期为"当日",因为系统跑批逻辑为T+1

  P_MMAPDM_CREATE_TMP_TABLES(TABLE_NAME , RETURN_NUM , RETURN_STATUS,TMP_PRE,TMP_CUR,TMP_INS,TMP_UPD); -- 调用创建临时表存储过程

  --恢复数据为前一日数据状态，为避免重新跑批引起错误
  DELETE FROM  DM_CUST_CARD_LVL_HIS   WHERE DM_START_DT = DM_TODAY;  -- 删除开始日期为"当日"的数据

  UPDATE DM_CUST_CARD_LVL_HIS SET DM_END_DT= DM_MAX_DATE  WHERE DM_END_DT=  DM_TODAY;  -- 更新结束日期为"当日"的数据为最大日期

  COMMIT;

  /* 取前一天数据  */
  DM_SQL := '  INSERT INTO  '||  TMP_PRE  || '
  (
     CUSTOMER_ID      --  客户号
    ,CIF_LVL_CODE     --客户级别_代码
    ,CIF_LVL          --客户级别
    ,CIF_LVL_DATE     --客户级别定级日期
    ,CARD_LVL_CODE    --客户卡级别_代码_借记卡
    ,CARD_LVL         --客户卡级别_借记卡
    ,CARD_DATE        --借记卡发卡日期
    ,ETL_DATE         --  跑批日期
    ,DM_START_DT      --  起始日期
    ,DM_END_DT        --  结束日期

  )
  SELECT
     CUSTOMER_ID      --  客户号
    ,CIF_LVL_CODE     --客户级别_代码
    ,CIF_LVL          --客户级别
    ,CIF_LVL_DATE     --客户级别定级日期
    ,CARD_LVL_CODE    --客户卡级别_代码_借记卡
    ,CARD_LVL         --客户卡级别_借记卡
    ,CARD_DATE        --借记卡发卡日期
    ,ETL_DATE         --  跑批日期
    ,DM_START_DT      --  起始日期
    ,DM_END_DT        --  结束日期
  FROM MMAPDM.DM_CUST_CARD_LVL_HIS
  WHERE DM_END_DT  = '  || DM_MAX_DATE ;
  EXECUTE  IMMEDIATE DM_SQL;
  COMMIT;
 
  /*取当日数据*/
  DM_SQL := 'INSERT INTO '|| TMP_CUR || '
  (
     CUSTOMER_ID      --  客户号
    ,CIF_LVL_CODE     --客户级别_代码
    ,CIF_LVL          --客户级别
    ,CIF_LVL_DATE     --客户级别定级日期
    ,CARD_LVL_CODE    --客户卡级别_代码_借记卡
    ,CARD_LVL         --客户卡级别_借记卡
    ,CARD_DATE        --借记卡发卡日期
    ,ETL_DATE         --  跑批日期
    ,DM_START_DT      --  起始日期
    ,DM_END_DT        --  结束日期
  )
  SELECT 
  a.CUSTOMER_ID AS CUSTOMER_ID
 ,c.VALUE_NUM  AS CIF_LVL_CODE
 ,c.ITEM_NM  AS CIF_LVL
 ,a.TX_DATE AS CIF_LVL_DATE
 ,NVL(b.VALUE_NUM,0) AS  CARD_LVL_CODE
 ,NVL(b.ITEM_NM,''未知'')  AS CARD_LVL
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
WHERE a.CUSTOMER_ID not in (''9999999999'')';

  EXECUTE  IMMEDIATE DM_SQL;
  COMMIT;

  /*cur-pre =  增量数据（包括修改后）*/
  DM_SQL := 'INSERT INTO '|| TMP_INS || '
  (
    CUSTOMER_ID      --  客户号
    ,CIF_LVL_CODE     --客户级别_代码
    ,CIF_LVL          --客户级别
    ,CIF_LVL_DATE     --客户级别定级日期
    ,CARD_LVL_CODE    --客户卡级别_代码_借记卡
    ,CARD_LVL         --客户卡级别_借记卡
    ,CARD_DATE        --借记卡发卡日期
    ,ETL_DATE         --  跑批日期
    ,DM_START_DT      --  起始日期
    ,DM_END_DT        --  结束日期
  )
  SELECT
    CUSTOMER_ID      --  客户号
    ,CIF_LVL_CODE     --客户级别_代码
    ,CIF_LVL          --客户级别
    ,CIF_LVL_DATE     --客户级别定级日期
    ,CARD_LVL_CODE    --客户卡级别_代码_借记卡
    ,CARD_LVL         --客户卡级别_借记卡
    ,CARD_DATE        --借记卡发卡日期
    ,ETL_DATE         --  跑批日期
    ,DM_START_DT      --  起始日期
    ,DM_END_DT        --  结束日期
  FROM '|| TMP_CUR ||  '  a
  WHERE
  NOT  EXISTS
  (
    SELECT
     CUSTOMER_ID      --  客户号
    ,CIF_LVL_CODE     --客户级别_代码
    ,CIF_LVL          --客户级别
    ,CIF_LVL_DATE     --客户级别定级日期
    ,CARD_LVL_CODE    --客户卡级别_代码_借记卡
    ,CARD_LVL         --客户卡级别_借记卡
    ,CARD_DATE        --借记卡发卡日期
    FROM '|| TMP_PRE ||  '  b
    WHERE
         TRIM(a.CUSTOMER_ID)= TRIM(b.CUSTOMER_ID)
    AND  TRIM(a.CIF_LVL_CODE)= TRIM(b.CIF_LVL_CODE)
    AND  TRIM(a.CIF_LVL)= TRIM(b.CIF_LVL)
    AND  TRIM(a.CIF_LVL_DATE)=TRIM(b.CIF_LVL_DATE)
    AND  TRIM(nvl(a.CARD_LVL_CODE,0))= TRIM(nvl(b.CARD_LVL_CODE,0))
    AND  TRIM(nvl(a.CARD_LVL,''未知''))=nvl( b.CARD_LVL,''未知'')
    AND  TRIM(nvl(a.CARD_DATE,To_date(99991231,''yyyy-mm-dd HH24:mi:ss'')))=TRIM(nvl(b.CARD_DATE,To_date(99991231,''yyyy-mm-dd HH24:mi:ss'')))
  )';
  EXECUTE  IMMEDIATE DM_SQL;
  COMMIT;

   /*  pre-cur  = 删数据（包括修改前） */
  DM_SQL := 'INSERT INTO '|| TMP_UPD || '
  (
     CUSTOMER_ID      --  客户号
    ,CIF_LVL_CODE     --客户级别_代码
    ,CIF_LVL          --客户级别
    ,CIF_LVL_DATE     --客户级别定级日期
    ,CARD_LVL_CODE    --客户卡级别_代码_借记卡
    ,CARD_LVL         --客户卡级别_借记卡
    ,CARD_DATE        --借记卡发卡日期
    ,ETL_DATE         --  跑批日期
    ,DM_START_DT      --  起始日期
    ,DM_END_DT        --  结束日期
  )
  SELECT
     CUSTOMER_ID      --  客户号
    ,CIF_LVL_CODE     --客户级别_代码
    ,CIF_LVL          --客户级别
    ,CIF_LVL_DATE     --客户级别定级日期
    ,CARD_LVL_CODE    --客户卡级别_代码_借记卡
    ,CARD_LVL         --客户卡级别_借记卡
    ,CARD_DATE        --借记卡发卡日期
    ,ETL_DATE         --  跑批日期
    ,DM_START_DT      --  起始日期
    ,DM_END_DT        --  结束日期
  FROM '|| TMP_PRE ||' a
  WHERE
  NOT  EXISTS
  (
    SELECT
     CUSTOMER_ID      --  客户号
    ,CIF_LVL_CODE     --客户级别_代码
    ,CIF_LVL          --客户级别
    ,CIF_LVL_DATE     --客户级别定级日期
    ,CARD_LVL_CODE    --客户卡级别_代码_借记卡
    ,CARD_LVL         --客户卡级别_借记卡
    ,CARD_DATE        --借记卡发卡日期
    ,ETL_DATE         --  跑批日期
    ,DM_START_DT      --  起始日期
    ,DM_END_DT        --  结束日期
    FROM '|| TMP_CUR ||  '  b
    WHERE
         TRIM(a.CUSTOMER_ID)= TRIM(b.CUSTOMER_ID)
    AND  TRIM(a.CIF_LVL_CODE)= TRIM(b.CIF_LVL_CODE)
    AND  TRIM(a.CIF_LVL)= TRIM(b.CIF_LVL)
    AND  TRIM(a.CIF_LVL_DATE)=TRIM(b.CIF_LVL_DATE)
    AND  TRIM(nvl(a.CARD_LVL_CODE,0))= TRIM(nvl(b.CARD_LVL_CODE,0))
    AND  TRIM(nvl(a.CARD_LVL,''未知''))=nvl( b.CARD_LVL,''未知'')
    AND  TRIM(nvl(a.CARD_DATE,To_date(99991231,''yyyy-mm-dd HH24:mi:ss'')))=TRIM(nvl(b.CARD_DATE,To_date(99991231,''yyyy-mm-dd HH24:mi:ss'')))
  )';
  EXECUTE  IMMEDIATE DM_SQL;
  COMMIT;

  /* 更新历史中上日数据的截止时间为当日时间(删除-插入替代更新) */
  DM_SQL := 'DELETE FROM MMAPDM.DM_CUST_CARD_LVL_HIS a
  WHERE
  EXISTS(
  SELECT
    CUSTOMER_ID      --  客户号
    ,CIF_LVL_CODE     --客户级别_代码
    ,CIF_LVL          --客户级别
    ,CIF_LVL_DATE     --客户级别定级日期
    ,CARD_LVL_CODE    --客户卡级别_代码_借记卡
    ,CARD_LVL         --客户卡级别_借记卡
    ,CARD_DATE        --借记卡发卡日期
    ,ETL_DATE         --  跑批日期
    ,DM_START_DT      --  起始日期
    ,DM_END_DT        --  结束日期
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
   CUSTOMER_ID      --  客户号
    ,CIF_LVL_CODE     --客户级别_代码
    ,CIF_LVL          --客户级别
    ,CIF_LVL_DATE     --客户级别定级日期
    ,CARD_LVL_CODE    --客户卡级别_代码_借记卡
    ,CARD_LVL         --客户卡级别_借记卡
    ,CARD_DATE        --借记卡发卡日期
    ,ETL_DATE         --  跑批日期
    ,DM_START_DT      --  起始日期
    ,DM_END_DT        --  结束日期
  )
  SELECT
   CUSTOMER_ID      --  客户号
    ,CIF_LVL_CODE     --客户级别_代码
    ,CIF_LVL          --客户级别
    ,CIF_LVL_DATE     --客户级别定级日期
    ,CARD_LVL_CODE    --客户卡级别_代码_借记卡
    ,CARD_LVL         --客户卡级别_借记卡
    ,CARD_DATE        --借记卡发卡日期
    ,ETL_DATE         --  跑批日期
    ,DM_START_DT      --  起始日期
    ,'|| DM_TODAY ||'
  FROM '|| TMP_UPD ||  '';
  EXECUTE  IMMEDIATE DM_SQL;


  /* 插入新增数据  */
  DM_SQL := 'INSERT INTO  MMAPDM.DM_CUST_CARD_LVL_HIS
  (
    CUSTOMER_ID      --  客户号
    ,CIF_LVL_CODE     --客户级别_代码
    ,CIF_LVL          --客户级别
    ,CIF_LVL_DATE     --客户级别定级日期
    ,CARD_LVL_CODE    --客户卡级别_代码_借记卡
    ,CARD_LVL         --客户卡级别_借记卡
    ,CARD_DATE        --借记卡发卡日期
    ,ETL_DATE         --  跑批日期
    ,DM_START_DT      --  起始日期
    ,DM_END_DT        --  结束日期
  )
  SELECT
   CUSTOMER_ID      --  客户号
    ,CIF_LVL_CODE     --客户级别_代码
    ,CIF_LVL          --客户级别
    ,CIF_LVL_DATE     --客户级别定级日期
    ,CARD_LVL_CODE    --客户卡级别_代码_借记卡
    ,CARD_LVL         --客户卡级别_借记卡
    ,CARD_DATE        --借记卡发卡日期
    ,ETL_DATE         --  跑批日期
    ,DM_START_DT      --  起始日期
    ,DM_END_DT        --  结束日期
  FROM '|| TMP_INS ||  '';
  EXECUTE  IMMEDIATE DM_SQL;

  IO_ROW := SQL%ROWCOUNT ;

  --P_MMAPDM_DROP_TMP_TABLES(TABLE_NAME,RETURN_NUM,RETURN_STATUS);

  SELECT SYSDATE INTO  V_END_TIMESTAMP  FROM dual;  -- 加载程序运行结束时间


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
