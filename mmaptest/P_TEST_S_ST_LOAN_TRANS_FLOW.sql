CREATE OR REPLACE PROCEDURE "MMAPTEST"."P_TEST_S_ST_LOAN_TRANS_FLOW"

AS
/* 定义变量*/
TRANS_NAME              VARCHAR2(50) :='P_TEST_S_ST_LOAN_TRANS_FLOW' ; --存储过程名
V_TABLE_NAME 	        VARCHAR2(50) :='ST_LOAN_TRANS_FLOW';  -- 测试目标表名
V_COL 			VARCHAR2(50);     --字段英文名
V_COL_NAME 		VARCHAR2(50);     --字段中文名
V_COL_DES 		VARCHAR2(50);     --字段描述
V_TEST_DES 		VARCHAR2(50);     --测试描述
V_TEST_STAGE 	        VARCHAR2(50) :='S_ST';  -- 测试阶段
V_TEST_TIMESTAMP        TIMESTAMP;  -- 加载测试时间

--存储过程日志变量
V_START_TIMESTAMP       TIMESTAMP;  -- 加载开始时间
V_END_TIMESTAMP         TIMESTAMP;  -- 加载结束时间
IO_ROW 			INTEGER;
IO_STATUS 		INTEGER;
VO_SQLERR 		VARCHAR2(5000);          -- 返回的描述

BEGIN
  SELECT SYSDATE INTO V_START_TIMESTAMP FROM dual;  -- 加载程序运行开始时间
  V_TEST_TIMESTAMP := V_START_TIMESTAMP;
  --1.字符型字段，由于字符型数据没有特殊处理，则仅测试表记录数
  V_COL := 'TABLE_COUNT';
  V_COL_NAME := '记录数';
  V_COL_DES := '表中记录数';
  V_TEST_DES := '计数';
  
/*VERIFY VARCHAR2 DATA TYPE: TOTLE RECODE*/
  INSERT INTO MMAPTEST.MMAP_TEST
  (
    TEST_DATE          --  测试时间
    ,TEST_STAGE        --  测试阶段
    ,TABLE_NAME      --  目标表名
    ,TX_DATE      --  数据日期
    ,COL      -- 字段英文名
    ,COL_NAME    --  字段中文名
    ,COL_DES    --  字段描述
    ,TEST_DES    --  测试描述
    ,VALUE_SOURCE    -- 字段值（源）
    ,VALUE_TARGET    --  字段值（目标）
    ,COMPARE_RESULT    --  对比结果
    ,REMARK    --  备注

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
select A.Tx_DT,COUNT(1) AS S_NUM    --修改
from   MMAPST.DMMKT_EVLNTRANS A
GROUP BY Tx_DT
) T1
FULL JOIN
(
SELECT A.TX_DATE,COUNT(1) AS ST_NUM  --修改
FROM   MMAPST.ST_LOAN_TRANS_FLOW A
GROUP BY TX_DATE
) T2
ON T1.Tx_DT=T2.TX_DATE;
  
/*VERIFY VARCHAR2 DATA TYPE: ACCTTYP_ID FIELD*/


/*VERIFY NUMBER DATA TYPE：TRANS_AMT1 */

V_COL := 'TRANS_AMT1';
V_COL_NAME := '当地币种交易余额';
V_COL_DES := '';
V_TEST_DES := 'SUM';

INSERT INTO MMAPTEST.MMAP_TEST
  (
    TEST_DATE          --  测试时间
    ,TEST_STAGE        --  测试阶段
    ,TABLE_NAME      --  目标表名
    ,TX_DATE      --  数据日期
    ,COL      -- 字段英文名
    ,COL_NAME    --  字段中文名
    ,COL_DES    --  字段描述
    ,TEST_DES    --  测试描述
    ,VALUE_SOURCE    -- 字段值（源）
    ,VALUE_TARGET    --  字段值（目标）
    ,COMPARE_RESULT    --  对比结果
    ,REMARK    --  备注

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
select A.Tx_DT,SUM(TRANS_AMT) AS S_NUM --修改
from   MMAPST.DMMKT_EVLNTRANS A
GROUP BY Tx_DT
) T1
FULL JOIN
(
SELECT A.TX_DATE,SUM(TRANS_AMT) AS ST_NUM  --修改
FROM   MMAPST.ST_LOAN_TRANS_FLOW A
GROUP BY TX_DATE
)T2
ON T1.Tx_DT=T2.TX_DATE;

/*WRITE TO LOG TABLE*/

IO_ROW := SQL%ROWCOUNT ; --记录存储过程记录数
COMMIT;
SELECT SYSDATE INTO  V_END_TIMESTAMP  FROM dual;  -- 加载程序运行结束时间
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
END P_TEST_S_ST_LOAN_TRANS_FLOW;