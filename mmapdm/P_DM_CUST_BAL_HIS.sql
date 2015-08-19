CREATE OR REPLACE PROCEDURE P_DM_CUST_BAL_HIS
(IO_STATUS OUT INTEGER,IO_SQLERR OUT VARCHAR2)
	AS
	TABLE_NAME VARCHAR2(125) :='DM_CUST_BAL_HIS';  -- 表名(修改)
	PROCEDURE_NAME VARCHAR2(125) :='P_'||TABLE_NAME;  -- 存储过程名(修改)

	IO_ROW INTEGER; --插入条数
	ST_ROW INTEGER;  --源数据条数
	
	RETURN_NUM NUMBER;                -- 返回状态值
	RETURN_STATUS VARCHAR2(5000);          -- 返回的描述
	
	V_ETL_DATE NUMBER;  -- 跑批日期
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

	SELECT SYSDATE INTO V_START_TIMESTAMP FROM DUAL;  -- 加载程序运行开始时间
	
	SELECT TO_NUMBER(TO_CHAR((SYSDATE),'YYYYMMDD')) INTO V_ETL_DATE FROM DUAL;  -- 取系统日期作为跑批日期
	
	SELECT TX_DATE INTO DM_TODAY FROM MMAPST.ST_SYSTEM_DATE;  -- 取数据日期
	
	--查询ST层表中是否有'当日'数据
	SELECT COUNT(1) INTO ST_ROW FROM MMAPST.ST_CUST_BAL WHERE PERIOD_ID=DM_TODAY;
	
	--如果ST层有"当日"数据，则进行数据抽取，否则保持DM层现有数据不变。
	IF ST_ROW>0
	THEN
		IO_ROW:=0;
		-- 调用创建临时表存储过程
		P_MMAPDM_CREATE_TMP_TABLES(TABLE_NAME , RETURN_NUM , RETURN_STATUS,TMP_PRE,TMP_CUR,TMP_INS,TMP_UPD); 

		--恢复数据为前一日数据状态，为避免重新跑批引起错误
		DELETE FROM  MMAPDM.DM_CUST_BAL_HIS   WHERE DM_START_DT = DM_TODAY;  -- 删除开始日期为"当日"的数据
		
		UPDATE MMAPDM.DM_CUST_BAL_HIS SET DM_END_DT= DM_MAX_DATE  WHERE DM_END_DT=  DM_TODAY;  -- 更新结束日期为"当日"的数据为最大日期
		
		COMMIT;

		/* 取前一天数据  */
		DM_SQL := '  INSERT INTO  '||  TMP_PRE  || '
		(
		  TX_DATE          --  数据日期
		  ,PERIOD_ID        --  日期
		  ,CUSTOMER_ID      --  客户号
		  ,CUST_CD_BAL_LC      --  活期存款余额（人民币）
		  ,CUST_TD_BAL_LC      --  定期存款余额（人民币）
		  ,CUST_LOAN_BAL_LC    --  贷款余额（人民币）
		  ,CUST_NDEBT_BAL_LC    --  国债余额（人民币）
		  ,CUST_FOUND_BAL_LC    --  基金余额（人民币）
		  ,CUST_FIN_BAL_LC    --  理财余额（人民币）
		  ,CUST_INSURE_BAL_LC    --  保险余额（人民币）
		  ,CUST_NOBLE_BAL_LC    --  贵金属余额（人民币）
		  ,CUST_CREDIT_BAL_LC    --  信用卡消费额（人民币）
		  ,CUST_ALL_BAL_LC    --  客户总资产余额（人民币）
		  ,CUST_CD_BAL_FC      --  活期存款余额（外币折合人民币）
		  ,CUST_TD_BAL_FC      --  定期存款余额（外币折合人民币）
		  ,CUST_LOAN_BAL_FC    --  贷款余额（外币折合人民币）
		  ,CUST_CREDIT_BAL_FC    --  信用卡消费额（外币折合人民币）
		  ,CUST_ALL_BAL_FC    --  客户总资产余额（外币折合人民币）
		  ,CUST_ALL_BAL      --  客户总资产余额（人民币+外币）
		  ,ETL_DATE        --  跑批日期
		  ,DM_START_DT      --  起始日期
		  ,DM_END_DT        --  结束日期
		
		)
		SELECT
		  TX_DATE          --  数据日期
		  ,PERIOD_ID        --  日期
		  ,CUSTOMER_ID      --  客户号
		  ,CUST_CD_BAL_LC      --  活期存款余额（人民币）
		  ,CUST_TD_BAL_LC      --  定期存款余额（人民币）
		  ,CUST_LOAN_BAL_LC    --  贷款余额（人民币）
		  ,CUST_NDEBT_BAL_LC    --  国债余额（人民币）
		  ,CUST_FOUND_BAL_LC    --  基金余额（人民币）
		  ,CUST_FIN_BAL_LC    --  理财余额（人民币）
		  ,CUST_INSURE_BAL_LC    --  保险余额（人民币）
		  ,CUST_NOBLE_BAL_LC    --  贵金属余额（人民币）
		  ,CUST_CREDIT_BAL_LC    --  信用卡消费额（人民币）
		  ,CUST_ALL_BAL_LC    --  客户总资产余额（人民币）
		  ,CUST_CD_BAL_FC      --  活期存款余额（外币折合人民币）
		  ,CUST_TD_BAL_FC      --  定期存款余额（外币折合人民币）
		  ,CUST_LOAN_BAL_FC    --  贷款余额（外币折合人民币）
		  ,CUST_CREDIT_BAL_FC    --  信用卡消费额（外币折合人民币）
		  ,CUST_ALL_BAL_FC    --  客户总资产余额（外币折合人民币）
		  ,CUST_ALL_BAL      --  客户总资产余额（人民币+外币）
		  ,ETL_DATE        --  跑批日期
		  ,DM_START_DT      --  起始日期
		  ,DM_END_DT        --  结束日期
		FROM MMAPDM.DM_CUST_BAL_HIS
		WHERE DM_END_DT  = '  || DM_MAX_DATE ;
		EXECUTE  IMMEDIATE DM_SQL;
		COMMIT;
		
		/*取当日数据*/
		DM_SQL := 'INSERT INTO '|| TMP_CUR || '
		(
		  TX_DATE          --  数据日期
		  ,PERIOD_ID        --  日期
		  ,CUSTOMER_ID      --  客户号
		  ,CUST_CD_BAL_LC      --  活期存款余额（人民币）
		  ,CUST_TD_BAL_LC      --  定期存款余额（人民币）
		  ,CUST_LOAN_BAL_LC    --  贷款余额（人民币）
		  ,CUST_NDEBT_BAL_LC    --  国债余额（人民币）
		  ,CUST_FOUND_BAL_LC    --  基金余额（人民币）
		  ,CUST_FIN_BAL_LC    --  理财余额（人民币）
		  ,CUST_INSURE_BAL_LC    --  保险余额（人民币）
		  ,CUST_NOBLE_BAL_LC    --  贵金属余额（人民币）
		  ,CUST_CREDIT_BAL_LC    --  信用卡消费额（人民币）
		  ,CUST_ALL_BAL_LC    --  客户总资产余额（人民币）
		  ,CUST_CD_BAL_FC      --  活期存款余额（外币折合人民币）
		  ,CUST_TD_BAL_FC      --  定期存款余额（外币折合人民币）
		  ,CUST_LOAN_BAL_FC    --  贷款余额（外币折合人民币）
		  ,CUST_CREDIT_BAL_FC    --  信用卡消费额（外币折合人民币）
		  ,CUST_ALL_BAL_FC    --  客户总资产余额（外币折合人民币）
		  ,CUST_ALL_BAL      --  客户总资产余额（人民币+外币）
		  ,ETL_DATE        --  跑批日期
		  ,DM_START_DT      --  起始日期
		  ,DM_END_DT        --  结束日期
		)
		SELECT
		  TX_DATE          --  数据日期
		  ,PERIOD_ID        --  日期
		  ,CUSTOMER_ID      --  客户号
		  ,CUST_CD_BAL_LC      --  活期存款余额（人民币）
		  ,CUST_TD_BAL_LC      --  定期存款余额（人民币）
		  ,CUST_LOAN_BAL_LC    --  贷款余额（人民币）
		  ,CUST_NDEBT_BAL_LC    --  国债余额（人民币）
		  ,CUST_FOUND_BAL_LC    --  基金余额（人民币）
		  ,CUST_FIN_BAL_LC    --  理财余额（人民币）
		  ,CUST_INSURE_BAL_LC    --  保险余额（人民币）
		  ,CUST_NOBLE_BAL_LC    --  贵金属余额（人民币）
		  ,CUST_CREDIT_BAL_LC    --  信用卡消费额（人民币）
		  ,CUST_ALL_BAL_LC    --  客户总资产余额（人民币）
		  ,CUST_CD_BAL_FC      --  活期存款余额（外币折合人民币）
		  ,CUST_TD_BAL_FC      --  定期存款余额（外币折合人民币）
		  ,CUST_LOAN_BAL_FC    --  贷款余额（外币折合人民币）
		  ,CUST_CREDIT_BAL_FC    --  信用卡消费额（外币折合人民币）
		  ,CUST_ALL_BAL_FC    --  客户总资产余额（外币折合人民币）
		  ,CUST_ALL_BAL      --  客户总资产余额（人民币+外币）
		  ,'||  V_ETL_DATE  ||'
		  ,'||  DM_TODAY ||  '
		  ,'||  DM_MAX_DATE  ||'
		FROM MMAPST.ST_CUST_BAL';
		EXECUTE  IMMEDIATE DM_SQL;
		COMMIT;
		
		/*cur-pre =  增量数据（包括修改后）*/
		DM_SQL := 'INSERT INTO '|| TMP_INS || '
		(
		  TX_DATE          --  数据日期
		  ,PERIOD_ID        --  日期
		  ,CUSTOMER_ID      --  客户号
		  ,CUST_CD_BAL_LC      --  活期存款余额（人民币）
		  ,CUST_TD_BAL_LC      --  定期存款余额（人民币）
		  ,CUST_LOAN_BAL_LC    --  贷款余额（人民币）
		  ,CUST_NDEBT_BAL_LC    --  国债余额（人民币）
		  ,CUST_FOUND_BAL_LC    --  基金余额（人民币）
		  ,CUST_FIN_BAL_LC    --  理财余额（人民币）
		  ,CUST_INSURE_BAL_LC    --  保险余额（人民币）
		  ,CUST_NOBLE_BAL_LC    --  贵金属余额（人民币）
		  ,CUST_CREDIT_BAL_LC    --  信用卡消费额（人民币）
		  ,CUST_ALL_BAL_LC    --  客户总资产余额（人民币）
		  ,CUST_CD_BAL_FC      --  活期存款余额（外币折合人民币）
		  ,CUST_TD_BAL_FC      --  定期存款余额（外币折合人民币）
		  ,CUST_LOAN_BAL_FC    --  贷款余额（外币折合人民币）
		  ,CUST_CREDIT_BAL_FC    --  信用卡消费额（外币折合人民币）
		  ,CUST_ALL_BAL_FC    --  客户总资产余额（外币折合人民币）
		  ,CUST_ALL_BAL      --  客户总资产余额（人民币+外币）
		  ,ETL_DATE        --  跑批日期
		  ,DM_START_DT      --  起始日期
		  ,DM_END_DT        --  结束日期
		)
		SELECT
		  TX_DATE          --  数据日期
		  ,PERIOD_ID        --  日期
		  ,CUSTOMER_ID      --  客户号
		  ,CUST_CD_BAL_LC      --  活期存款余额（人民币）
		  ,CUST_TD_BAL_LC      --  定期存款余额（人民币）
		  ,CUST_LOAN_BAL_LC    --  贷款余额（人民币）
		  ,CUST_NDEBT_BAL_LC    --  国债余额（人民币）
		  ,CUST_FOUND_BAL_LC    --  基金余额（人民币）
		  ,CUST_FIN_BAL_LC    --  理财余额（人民币）
		  ,CUST_INSURE_BAL_LC    --  保险余额（人民币）
		  ,CUST_NOBLE_BAL_LC    --  贵金属余额（人民币）
		  ,CUST_CREDIT_BAL_LC    --  信用卡消费额（人民币）
		  ,CUST_ALL_BAL_LC    --  客户总资产余额（人民币）
		  ,CUST_CD_BAL_FC      --  活期存款余额（外币折合人民币）
		  ,CUST_TD_BAL_FC      --  定期存款余额（外币折合人民币）
		  ,CUST_LOAN_BAL_FC    --  贷款余额（外币折合人民币）
		  ,CUST_CREDIT_BAL_FC    --  信用卡消费额（外币折合人民币）
		  ,CUST_ALL_BAL_FC    --  客户总资产余额（外币折合人民币）
		  ,CUST_ALL_BAL      --  客户总资产余额（人民币+外币）
		  ,ETL_DATE        --  跑批日期
		  ,DM_START_DT      --  起始日期
		  ,DM_END_DT        --  结束日期
		FROM '|| TMP_CUR ||  '  a
		WHERE
		NOT  EXISTS
		(
		  SELECT
		    CUSTOMER_ID        --  客户号
		    ,CUST_CD_BAL_LC      --  活期存款余额（人民币）
		    ,CUST_TD_BAL_LC      --  定期存款余额（人民币）
		    ,CUST_LOAN_BAL_LC    --  贷款余额（人民币）
		    ,CUST_NDEBT_BAL_LC    --  国债余额（人民币）
		    ,CUST_FOUND_BAL_LC    --  基金余额（人民币）
		    ,CUST_FIN_BAL_LC    --  理财余额（人民币）
		    ,CUST_INSURE_BAL_LC    --  保险余额（人民币）
		    ,CUST_NOBLE_BAL_LC    --  贵金属余额（人民币）
		    ,CUST_CREDIT_BAL_LC    --  信用卡消费额（人民币）
		    ,CUST_ALL_BAL_LC    --  客户总资产余额（人民币）
		    ,CUST_CD_BAL_FC      --  活期存款余额（外币折合人民币）
		    ,CUST_TD_BAL_FC      --  定期存款余额（外币折合人民币）
		    ,CUST_LOAN_BAL_FC    --  贷款余额（外币折合人民币）
		    ,CUST_CREDIT_BAL_FC    --  信用卡消费额（外币折合人民币）
		    ,CUST_ALL_BAL_FC    --  客户总资产余额（外币折合人民币）
		    ,CUST_ALL_BAL      --  客户总资产余额（人民币+外币）
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
		
		 /*  pre-cur  = 删数据（包括修改前） */
		DM_SQL := 'INSERT INTO '|| TMP_UPD || '
		(
		  TX_DATE          --  数据日期
		  ,PERIOD_ID        --  日期
		  ,CUSTOMER_ID      --  客户号
		  ,CUST_CD_BAL_LC      --  活期存款余额（人民币）
		  ,CUST_TD_BAL_LC      --  定期存款余额（人民币）
		  ,CUST_LOAN_BAL_LC    --  贷款余额（人民币）
		  ,CUST_NDEBT_BAL_LC    --  国债余额（人民币）
		  ,CUST_FOUND_BAL_LC    --  基金余额（人民币）
		  ,CUST_FIN_BAL_LC    --  理财余额（人民币）
		  ,CUST_INSURE_BAL_LC    --  保险余额（人民币）
		  ,CUST_NOBLE_BAL_LC    --  贵金属余额（人民币）
		  ,CUST_CREDIT_BAL_LC    --  信用卡消费额（人民币）
		  ,CUST_ALL_BAL_LC    --  客户总资产余额（人民币）
		  ,CUST_CD_BAL_FC      --  活期存款余额（外币折合人民币）
		  ,CUST_TD_BAL_FC      --  定期存款余额（外币折合人民币）
		  ,CUST_LOAN_BAL_FC    --  贷款余额（外币折合人民币）
		  ,CUST_CREDIT_BAL_FC    --  信用卡消费额（外币折合人民币）
		  ,CUST_ALL_BAL_FC    --  客户总资产余额（外币折合人民币）
		  ,CUST_ALL_BAL      --  客户总资产余额（人民币+外币）
		  ,ETL_DATE        --  跑批日期
		  ,DM_START_DT      --  起始日期
		  ,DM_END_DT        --  结束日期
		)
		SELECT
		  TX_DATE          --  数据日期
		  ,PERIOD_ID        --  日期
		  ,CUSTOMER_ID      --  客户号
		  ,CUST_CD_BAL_LC      --  活期存款余额（人民币）
		  ,CUST_TD_BAL_LC      --  定期存款余额（人民币）
		  ,CUST_LOAN_BAL_LC    --  贷款余额（人民币）
		  ,CUST_NDEBT_BAL_LC    --  国债余额（人民币）
		  ,CUST_FOUND_BAL_LC    --  基金余额（人民币）
		  ,CUST_FIN_BAL_LC    --  理财余额（人民币）
		  ,CUST_INSURE_BAL_LC    --  保险余额（人民币）
		  ,CUST_NOBLE_BAL_LC    --  贵金属余额（人民币）
		  ,CUST_CREDIT_BAL_LC    --  信用卡消费额（人民币）
		  ,CUST_ALL_BAL_LC    --  客户总资产余额（人民币）
		  ,CUST_CD_BAL_FC      --  活期存款余额（外币折合人民币）
		  ,CUST_TD_BAL_FC      --  定期存款余额（外币折合人民币）
		  ,CUST_LOAN_BAL_FC    --  贷款余额（外币折合人民币）
		  ,CUST_CREDIT_BAL_FC    --  信用卡消费额（外币折合人民币）
		  ,CUST_ALL_BAL_FC    --  客户总资产余额（外币折合人民币）
		  ,CUST_ALL_BAL      --  客户总资产余额（人民币+外币）
		  ,ETL_DATE        --  跑批日期
		  ,DM_START_DT      --  起始日期
		  ,DM_END_DT        --  结束日期
		FROM '|| TMP_PRE ||' a
		WHERE
		NOT  EXISTS
		(
		  SELECT
		    TX_DATE          --  数据日期
		    ,PERIOD_ID        --  日期
		    ,CUSTOMER_ID      --  客户号
		    ,CUST_CD_BAL_LC      --  活期存款余额（人民币）
		    ,CUST_TD_BAL_LC      --  定期存款余额（人民币）
		    ,CUST_LOAN_BAL_LC    --  贷款余额（人民币）
		    ,CUST_NDEBT_BAL_LC    --  国债余额（人民币）
		    ,CUST_FOUND_BAL_LC    --  基金余额（人民币）
		    ,CUST_FIN_BAL_LC    --  理财余额（人民币）
		    ,CUST_INSURE_BAL_LC    --  保险余额（人民币）
		    ,CUST_NOBLE_BAL_LC    --  贵金属余额（人民币）
		    ,CUST_CREDIT_BAL_LC    --  信用卡消费额（人民币）
		    ,CUST_ALL_BAL_LC    --  客户总资产余额（人民币）
		    ,CUST_CD_BAL_FC      --  活期存款余额（外币折合人民币）
		    ,CUST_TD_BAL_FC      --  定期存款余额（外币折合人民币）
		    ,CUST_LOAN_BAL_FC    --  贷款余额（外币折合人民币）
		    ,CUST_CREDIT_BAL_FC    --  信用卡消费额（外币折合人民币）
		    ,CUST_ALL_BAL_FC    --  客户总资产余额（外币折合人民币）
		    ,CUST_ALL_BAL      --  客户总资产余额（人民币+外币）
		    ,ETL_DATE        --  跑批日期
		    ,DM_START_DT      --  起始日期
		    ,DM_END_DT        --  结束日期
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
		
		/* 更新历史中上日数据的截止时间为当日时间(删除-插入替代更新) */
		DM_SQL := 'DELETE FROM MMAPDM.DM_CUST_BAL_HIS A
		WHERE
		EXISTS(
		SELECT
		  TX_DATE          --  数据日期
		  ,PERIOD_ID        --  日期
		  ,CUSTOMER_ID      --  客户号
		  ,CUST_CD_BAL_LC      --  活期存款余额（人民币）
		  ,CUST_TD_BAL_LC      --  定期存款余额（人民币）
		  ,CUST_LOAN_BAL_LC    --  贷款余额（人民币）
		  ,CUST_NDEBT_BAL_LC    --  国债余额（人民币）
		  ,CUST_FOUND_BAL_LC    --  基金余额（人民币）
		  ,CUST_FIN_BAL_LC    --  理财余额（人民币）
		  ,CUST_INSURE_BAL_LC    --  保险余额（人民币）
		  ,CUST_NOBLE_BAL_LC    --  贵金属余额（人民币）
		  ,CUST_CREDIT_BAL_LC    --  信用卡消费额（人民币）
		  ,CUST_ALL_BAL_LC    --  客户总资产余额（人民币）
		  ,CUST_CD_BAL_FC      --  活期存款余额（外币折合人民币）
		  ,CUST_TD_BAL_FC      --  定期存款余额（外币折合人民币）
		  ,CUST_LOAN_BAL_FC    --  贷款余额（外币折合人民币）
		  ,CUST_CREDIT_BAL_FC    --  信用卡消费额（外币折合人民币）
		  ,CUST_ALL_BAL_FC    --  客户总资产余额（外币折合人民币）
		  ,CUST_ALL_BAL      --  客户总资产余额（人民币+外币）
		  ,ETL_DATE        --  跑批日期
		  ,DM_START_DT      --  起始日期
		  ,DM_END_DT        --  结束日期
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
		  TX_DATE          --  数据日期
		  ,PERIOD_ID        --  日期
		  ,CUSTOMER_ID      --  客户号
		  ,CUST_CD_BAL_LC      --  活期存款余额（人民币）
		  ,CUST_TD_BAL_LC      --  定期存款余额（人民币）
		  ,CUST_LOAN_BAL_LC    --  贷款余额（人民币）
		  ,CUST_NDEBT_BAL_LC    --  国债余额（人民币）
		  ,CUST_FOUND_BAL_LC    --  基金余额（人民币）
		  ,CUST_FIN_BAL_LC    --  理财余额（人民币）
		  ,CUST_INSURE_BAL_LC    --  保险余额（人民币）
		  ,CUST_NOBLE_BAL_LC    --  贵金属余额（人民币）
		  ,CUST_CREDIT_BAL_LC    --  信用卡消费额（人民币）
		  ,CUST_ALL_BAL_LC    --  客户总资产余额（人民币）
		  ,CUST_CD_BAL_FC      --  活期存款余额（外币折合人民币）
		  ,CUST_TD_BAL_FC      --  定期存款余额（外币折合人民币）
		  ,CUST_LOAN_BAL_FC    --  贷款余额（外币折合人民币）
		  ,CUST_CREDIT_BAL_FC    --  信用卡消费额（外币折合人民币）
		  ,CUST_ALL_BAL_FC    --  客户总资产余额（外币折合人民币）
		  ,CUST_ALL_BAL      --  客户总资产余额（人民币+外币）
		  ,ETL_DATE        --  跑批日期
		  ,DM_START_DT      --  起始日期
		  ,DM_END_DT        --  结束日期
		)
		SELECT
		  TX_DATE          --  数据日期
		  ,PERIOD_ID        --  日期
		  ,CUSTOMER_ID      --  客户号
		  ,CUST_CD_BAL_LC      --  活期存款余额（人民币）
		  ,CUST_TD_BAL_LC      --  定期存款余额（人民币）
		  ,CUST_LOAN_BAL_LC    --  贷款余额（人民币）
		  ,CUST_NDEBT_BAL_LC    --  国债余额（人民币）
		  ,CUST_FOUND_BAL_LC    --  基金余额（人民币）
		  ,CUST_FIN_BAL_LC    --  理财余额（人民币）
		  ,CUST_INSURE_BAL_LC    --  保险余额（人民币）
		  ,CUST_NOBLE_BAL_LC    --  贵金属余额（人民币）
		  ,CUST_CREDIT_BAL_LC    --  信用卡消费额（人民币）
		  ,CUST_ALL_BAL_LC    --  客户总资产余额（人民币）
		  ,CUST_CD_BAL_FC      --  活期存款余额（外币折合人民币）
		  ,CUST_TD_BAL_FC      --  定期存款余额（外币折合人民币）
		  ,CUST_LOAN_BAL_FC    --  贷款余额（外币折合人民币）
		  ,CUST_CREDIT_BAL_FC    --  信用卡消费额（外币折合人民币）
		  ,CUST_ALL_BAL_FC    --  客户总资产余额（外币折合人民币）
		  ,CUST_ALL_BAL      --  客户总资产余额（人民币+外币）
		  ,ETL_DATE        --  跑批日期
		  ,DM_START_DT      --  起始日期
		  ,'|| DM_TODAY ||'
		FROM '|| TMP_UPD ||  '';
		EXECUTE  IMMEDIATE DM_SQL;
		IO_ROW := IO_ROW+SQL%ROWCOUNT ;
		
		/* 插入新增数据  */
		DM_SQL := 'INSERT INTO  MMAPDM.DM_CUST_BAL_HIS
		(
		  TX_DATE          --  数据日期
		  ,PERIOD_ID        --  日期
		  ,CUSTOMER_ID      --  客户号
		  ,CUST_CD_BAL_LC      --  活期存款余额（人民币）
		  ,CUST_TD_BAL_LC      --  定期存款余额（人民币）
		  ,CUST_LOAN_BAL_LC    --  贷款余额（人民币）
		  ,CUST_NDEBT_BAL_LC    --  国债余额（人民币）
		  ,CUST_FOUND_BAL_LC    --  基金余额（人民币）
		  ,CUST_FIN_BAL_LC    --  理财余额（人民币）
		  ,CUST_INSURE_BAL_LC    --  保险余额（人民币）
		  ,CUST_NOBLE_BAL_LC    --  贵金属余额（人民币）
		  ,CUST_CREDIT_BAL_LC    --  信用卡消费额（人民币）
		  ,CUST_ALL_BAL_LC    --  客户总资产余额（人民币）
		  ,CUST_CD_BAL_FC      --  活期存款余额（外币折合人民币）
		  ,CUST_TD_BAL_FC      --  定期存款余额（外币折合人民币）
		  ,CUST_LOAN_BAL_FC    --  贷款余额（外币折合人民币）
		  ,CUST_CREDIT_BAL_FC    --  信用卡消费额（外币折合人民币）
		  ,CUST_ALL_BAL_FC    --  客户总资产余额（外币折合人民币）
		  ,CUST_ALL_BAL      --  客户总资产余额（人民币+外币）
		  ,ETL_DATE        --  跑批日期
		  ,DM_START_DT      --  起始日期
		  ,DM_END_DT        --  结束日期
		)
		SELECT
		  TX_DATE          --  数据日期
		  ,PERIOD_ID        --  日期
		  ,CUSTOMER_ID      --  客户号
		  ,CUST_CD_BAL_LC      --  活期存款余额（人民币）
		  ,CUST_TD_BAL_LC      --  定期存款余额（人民币）
		  ,CUST_LOAN_BAL_LC    --  贷款余额（人民币）
		  ,CUST_NDEBT_BAL_LC    --  国债余额（人民币）
		  ,CUST_FOUND_BAL_LC    --  基金余额（人民币）
		  ,CUST_FIN_BAL_LC    --  理财余额（人民币）
		  ,CUST_INSURE_BAL_LC    --  保险余额（人民币）
		  ,CUST_NOBLE_BAL_LC    --  贵金属余额（人民币）
		  ,CUST_CREDIT_BAL_LC    --  信用卡消费额（人民币）
		  ,CUST_ALL_BAL_LC    --  客户总资产余额（人民币）
		  ,CUST_CD_BAL_FC      --  活期存款余额（外币折合人民币）
		  ,CUST_TD_BAL_FC      --  定期存款余额（外币折合人民币）
		  ,CUST_LOAN_BAL_FC    --  贷款余额（外币折合人民币）
		  ,CUST_CREDIT_BAL_FC    --  信用卡消费额（外币折合人民币）
		  ,CUST_ALL_BAL_FC    --  客户总资产余额（外币折合人民币）
		  ,CUST_ALL_BAL      --  客户总资产余额（人民币+外币）
		  ,ETL_DATE        --  跑批日期
		  ,DM_START_DT      --  起始日期
		  ,DM_END_DT        --  结束日期
		FROM '|| TMP_INS ||  '';
		EXECUTE  IMMEDIATE DM_SQL;

		IO_ROW := IO_ROW+SQL%ROWCOUNT ;

		P_MMAPDM_DROP_TMP_TABLES(TABLE_NAME,RETURN_NUM,RETURN_STATUS);
	END IF;

	SELECT SYSDATE INTO  V_END_TIMESTAMP  FROM dual;  -- 加载程序运行结束时间

	/*写日志*/
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
