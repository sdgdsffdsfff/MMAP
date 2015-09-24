CREATE OR REPLACE PROCEDURE "P_TEST_S_ST_ACCOUNT_INVEST"
  AS
  
TRANS_NAME VARCHAR2(50) :='P_TEST_S_ST_ACCOUNT_INVEST' ; --存储过程名

V_TABLE_NAME VARCHAR2(50) :='ST_ACCOUNT_INVEST';  -- 测试目标表名
V_COL VARCHAR2(50);     --字段英文名
V_COL_NAME VARCHAR2(50);     --字段中文名
V_COL_DES VARCHAR2(50);     --字段描述
V_TEST_DES VARCHAR2(50);     --测试描述
V_TEST_STAGE VARCHAR2(50) :='S-ST';  -- 测试阶段
V_TEST_TIMESTAMP TIMESTAMP;  -- 加载测试时间

--存储过程日志变量
V_START_TIMESTAMP TIMESTAMP;  -- 加载开始时间
V_END_TIMESTAMP    TIMESTAMP;  -- 加载结束时间
IO_ROW INTEGER;
IO_STATUS INTEGER;
VO_SQLERR VARCHAR2(5000);          -- 返回的描述

BEGIN
SELECT SYSDATE INTO V_START_TIMESTAMP FROM dual;  -- 加载程序运行开始时间
V_TEST_TIMESTAMP := V_START_TIMESTAMP;

--1.字符型字段
--如果表中的字符型数据没有特殊处理，则仅测试表记录数即可;如果有特殊处理，则再做个别字段逻辑测试。
--表记录数
V_COL := 'TABLE_COUNT';
V_COL_NAME := '记录数';
V_COL_DES := '表中记录数';
V_TEST_DES := '计数';

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
,T1.Source_NUM
,T2.ST_NUM
,NVL(T1.Source_NUM,0)-nvl(T2.ST_NUM,0)
,NULL
FROM 
(
select A.Tx_DT,COUNT(1) AS Source_NUM    --源修改
from   (
select
    to_number(to_char(SYSDATE,'YYYYMMDD')),
    a.TX_DT,
    to_number(to_char(a.TX_DT,'YYYYMMDD')),
    TRIM(a.CIF_ORG),
    TRIM(a.Acc_ORG),
    TRIM(a.THIRDPART_ORG),
    null,
    TRIM(A.AccSts_ID),
    TRIM(a.Brnh_ID),
    TRIM(a.EDeal_DT),
    null,
    TRIM(b.ProdTyp_ID),
    TRIM(b.Face_Amt)
 from
    MMAPST.DMMKT_ACEBond a INNER JOIN MMAPST.DMMKT_ACEBondAmt b
    ON a.THIRDPART_ORG=b.THIRDPART_ORG
    AND a.TX_DT=b.TX_DT
union all
 select
    to_number(to_char(SYSDATE,'YYYYMMDD')),
    c.TX_DT,
    to_number(to_char(c.TX_DT,'YYYYMMDD')),
    TRIM(c.CIF_ORG),
    TRIM(c.Acc_Org),
    TRIM(d.ThirdPart_Org),
    null,
    TRIM(d.AccSts_ID),
    TRIM(d.BRNH_ID),
    TRIM(d.Deal_DT),
    null,
    TRIM(c.ProdTyp_ID),
    TRIM(c.Vol_Amt)
 from
    MMAPST.DMMKT_ACFSShare c INNER JOIN MMAPST.DMMKT_ACFS d
    ON c.ThirdPart_Org=D.ThirdPart_Org
    AND c.CIF_Org=d.CIF_Org
    AND c.TX_DT=d.TX_DT
) A
GROUP BY Tx_DT
) T1
FULL JOIN 
(
SELECT A.TX_DATE,COUNT(1) AS ST_NUM  --目标修改
FROM   MMAPST.ST_ACCOUNT_INVEST A
GROUP BY TX_DATE
)T2
ON T1.Tx_DT=T2.TX_DATE;

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
  


END  P_TEST_S_ST_ACCOUNT_INVEST;
/
