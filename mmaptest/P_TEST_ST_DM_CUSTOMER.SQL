CREATE OR REPLACE PROCEDURE "P_TEST_ST_DM_CUSTOMER"

  AS
TRANS_NAME VARCHAR2(50) :='P_TEST_ST_DM_CUSTOMER' ; --存储过程名

V_TABLE_NAME VARCHAR2(50) :='DM_CUSTOMER';  -- 测试目标表名
V_COL VARCHAR2(50);     --字段英文名
V_COL_NAME VARCHAR2(50);     --字段中文名
V_COL_DES VARCHAR2(50);     --字段描述
V_TEST_DES VARCHAR2(50);     --测试描述
V_TEST_STAGE VARCHAR2(50) :='ST_DM';  -- 测试阶段
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

--2.字符型字段
--如果表中的字符型数据没有特殊处理，则仅测试表记录数即可;如果有特殊处理，则再做个别字段逻辑测试。
--表记录数
V_COL := 'GENDER';
V_COL_NAME := '性别';
V_COL_DES := '按性别分类计数';
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
--3.字符型字段
--如果表中的字符型数据没有特殊处理，则仅测试表记录数即可;如果有特殊处理，则再做个别字段逻辑测试。
--表记录数
V_COL := 'EDU_CODE';
V_COL_NAME := '教育程度';
V_COL_DES := '按教育程度分类计数';
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

--4.字符型字段
--如果表中的字符型数据没有特殊处理，则仅测试表记录数即可;如果有特殊处理，则再做个别字段逻辑测试。
--表记录数
V_COL := 'MARITALSTS_CODE';
V_COL_NAME := '婚姻状态';
V_COL_DES := '按婚姻状态分类计数';
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


--5.字符型字段
--如果表中的字符型数据没有特殊处理，则仅测试表记录数即可;如果有特殊处理，则再做个别字段逻辑测试。
--表记录数
V_COL := 'OCCP_CODE';
V_COL_NAME := '职业';
V_COL_DES := '按职业分类计数';
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

--6.字符型字段
--如果表中的字符型数据没有特殊处理，则仅测试表记录数即可;如果有特殊处理，则再做个别字段逻辑测试。
--表记录数
V_COL := 'POSITION_CODE';
V_COL_NAME := '职位';
V_COL_DES := '按职业位分类计数';
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

--7.字符型字段
--如果表中的字符型数据没有特殊处理，则仅测试表记录数即可;如果有特殊处理，则再做个别字段逻辑测试。
--表记录数
V_COL := 'COUNTRY_CODE';
V_COL_NAME := '国家';
V_COL_DES := '按国家分类计数';
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

--8.字符型字段
--如果表中的字符型数据没有特殊处理，则仅测试表记录数即可;如果有特殊处理，则再做个别字段逻辑测试。
--表记录数
V_COL := 'RACE_CODE';
V_COL_NAME := '种族';
V_COL_DES := '按种族分类计数';
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

--9.字符型字段
--如果表中的字符型数据没有特殊处理，则仅测试表记录数即可;如果有特殊处理，则再做个别字段逻辑测试。
--表记录数
V_COL := 'CIF_LVL_CODE';
V_COL_NAME := '客户级别';
V_COL_DES := '按客户级别分类计数';
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

--10.字符型字段
--如果表中的字符型数据没有特殊处理，则仅测试表记录数即可;如果有特殊处理，则再做个别字段逻辑测试。
--表记录数
V_COL := 'RISK_LVL_CODE';
V_COL_NAME := '风险等级';
V_COL_DES := '按风险等级分类计数';
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


IO_ROW := SQL%ROWCOUNT ; --记录存储过程记录数
COMMIT;
SELECT SYSDATE INTO  V_END_TIMESTAMP  FROM dual;  -- 加载程序运行结束时间
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
