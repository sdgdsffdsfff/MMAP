CREATE OR REPLACE PROCEDURE P_DM_CUST_CARD_LVL_HIS
(IO_STATUS OUT INTEGER,IO_SQLERR OUT VARCHAR2)
AS
	TABLE_NAME VARCHAR2(125) :='DM_CUST_CARD_LVL_HIS';  -- 表名(修改)
	PROCEDURE_NAME VARCHAR2(125) :='P_'||TABLE_NAME;  -- 存储过程名(修改)

	IO_ROW INTEGER; --插入条数
	ST_ROW1 INTEGER;  --源1数据条数
	ST_ROW2 INTEGER;  --源2数据条数

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
	SELECT COUNT(1) INTO ST_ROW1 FROM MMAPST.ST_CUST_CARD WHERE PERIOD_ID=DM_TODAY;
	SELECT COUNT(1) INTO ST_ROW2 FROM MMAPST.ST_CUSTOMER WHERE PERIOD_ID=DM_TODAY;

	--如果ST层有"当日"数据，则进行数据抽取，否则保持DM层现有数据不变。
	IF ST_ROW1>0 OR ST_ROW2>0
	THEN
		IO_ROW:=0;
		P_MMAPDM_CREATE_TMP_TABLES(TABLE_NAME , RETURN_NUM , RETURN_STATUS,TMP_PRE,TMP_CUR,TMP_INS,TMP_UPD); -- 调用创建临时表存储过程

		--恢复数据为前一日数据状态，为避免重新跑批引起错误
		DELETE FROM  MMAPDM.DM_CUST_CARD_LVL_HIS   WHERE DM_START_DT = DM_TODAY;  -- 删除开始日期为"当日"的数据

		UPDATE MMAPDM.DM_CUST_CARD_LVL_HIS SET DM_END_DT = DM_MAX_DATE  WHERE DM_END_DT = DM_TODAY;  -- 更新结束日期为"当日"的数据为最大日期

		COMMIT;

		/* 取前一天数据  */
		DM_SQL := '  INSERT INTO  '||  TMP_PRE  || '
		(
		   CUSTOMER_ID      --客户号
		  ,CIF_LVL_CODE     --客户级别_代码
		  ,CIF_LVL          --客户级别
		  ,CIF_LVL_DATE     --客户级别定级日期
		  ,CARD_LVL_CODE    --客户卡级别_代码_借记卡
		  ,CARD_LVL         --客户卡级别_借记卡
		  ,CARD_DATE        --借记卡发卡日期
		  ,ETL_DATE         --跑批日期
		  ,DM_START_DT      --起始日期
		  ,DM_END_DT        --结束日期
		)
		SELECT
		   CUSTOMER_ID      --客户号
		  ,CIF_LVL_CODE     --客户级别_代码
		  ,CIF_LVL          --客户级别
		  ,CIF_LVL_DATE     --客户级别定级日期
		  ,CARD_LVL_CODE    --客户卡级别_代码_借记卡
		  ,CARD_LVL         --客户卡级别_借记卡
		  ,CARD_DATE        --借记卡发卡日期
		  ,ETL_DATE         --跑批日期
		  ,DM_START_DT      --起始日期
		  ,DM_END_DT        --结束日期
		FROM MMAPDM.DM_CUST_CARD_LVL_HIS
		WHERE DM_END_DT  = ' || DM_MAX_DATE ;
		EXECUTE IMMEDIATE DM_SQL;
		COMMIT;

		/*取当日数据*/
		DM_SQL := 'INSERT INTO '|| TMP_CUR || '
		(
		   CUSTOMER_ID      --客户号
		  ,CIF_LVL_CODE     --客户级别_代码
		  ,CIF_LVL          --客户级别
		  ,CIF_LVL_DATE     --客户级别定级日期
		  ,CARD_LVL_CODE    --客户卡级别_代码_借记卡
		  ,CARD_LVL         --客户卡级别_借记卡
		  ,CARD_DATE        --借记卡发卡日期
		  ,ETL_DATE         --跑批日期
		  ,DM_START_DT      --起始日期
		  ,DM_END_DT        --结束日期
		)
		SELECT
		 A.CUSTOMER_ID AS CUSTOMER_ID
		,NVL(C.VALUE_NUM,0) AS CIF_LVL_CODE
		,NVL(C.ITEM_NM,''未知'')  AS CIF_LVL
		,A.TX_DATE AS CIF_LVL_DATE
		,NVL(B.VALUE_NUM,0) AS  CARD_LVL_CODE
		,NVL(B.ITEM_NM,''未知'')  AS CARD_LVL
		,B.ISSU_DT AS CARD_DATE
		,'||  V_ETL_DATE  ||'
		,'||  DM_TODAY ||  '
		,'||  DM_MAX_DATE  ||'
		FROM MMAPST.ST_CUSTOMER A
		LEFT JOIN
		(
			SELECT CUSTOMER_ID,VALUE_NUM,ITEM_NM,ISSU_DT
			FROM  (
				SELECT	ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY NVL(B.VALUE_NUM,0) DESC,ISSU_DT ASC) AS ROW_NUM
					,A.CUSTOMER_ID
					,A.CRDTYP_ID
					,NVL(b.ITEM_NM,''未知'') AS ITEM_NM
					,NVL(B.VALUE_NUM,0) AS VALUE_NUM
					,A.ISSU_DT
				FROM	MMAPST.ST_CUST_CARD A
				LEFT JOIN
				(
					SELECT	SOURCE_ITEM_ID
						,SOURCE_ITEM_NM
						,ITEM_ID
						,ITEM_NM
						,VALUE_NUM
					FROM MMAPST.ST_ITEM
					WHERE ITEM_TYP=''CARD_LVL''
				) B
				ON
				A.CRDTYP_ID=B.SOURCE_ITEM_ID
				) T
			WHERE T. ROW_NUM=1
		) B
		ON A.CUSTOMER_ID=B.CUSTOMER_ID
		LEFT JOIN
		(SELECT SOURCE_ITEM_ID,SOURCE_ITEM_NM,ITEM_ID,ITEM_NM,VALUE_NUM FROM MMAPST.ST_ITEM WHERE ITEM_TYP=''CIF_LVL'') C
		ON A.CIF_LVL_CODE=C.SOURCE_ITEM_ID';
		EXECUTE  IMMEDIATE DM_SQL;
		COMMIT;
		
		/*cur-pre =  增量数据（包括修改后）*/
		DM_SQL := 'INSERT INTO '|| TMP_INS || '
		(
		CUSTOMER_ID       --客户号
		,CIF_LVL_CODE     --客户级别_代码
		,CIF_LVL          --客户级别
		,CIF_LVL_DATE     --客户级别定级日期
		,CARD_LVL_CODE    --客户卡级别_代码_借记卡
		,CARD_LVL         --客户卡级别_借记卡
		,CARD_DATE        --借记卡发卡日期
		,ETL_DATE         --跑批日期
		,DM_START_DT      --起始日期
		,DM_END_DT        --结束日期
		)
		
		SELECT
			 A.CUSTOMER_ID      --客户号
			,A.CIF_LVL_CODE     --客户级别_代码
			,A.CIF_LVL          --客户级别
			,CASE WHEN C.CIF_LVL_CODE=A.CIF_LVL_CODE THEN  C.CIF_LVL_DATE
			ELSE A.CIF_LVL_DATE  
			END AS CIF_LVL_DATE --客户级别定级日期
			,A.CARD_LVL_CODE    --客户卡级别_代码_借记卡
			,A.CARD_LVL         --客户卡级别_借记卡
			,A.CARD_DATE        --借记卡发卡日期
			,A.ETL_DATE         --跑批日期
			,A.DM_START_DT      --起始日期
			,A.DM_END_DT        --结束日期
		
		FROM
		(
			SELECT
				 CUSTOMER_ID      --客户号
				,CIF_LVL_CODE     --客户级别_代码
				,CIF_LVL          --客户级别
				,CIF_LVL_DATE     --客户级别定级日期
				,CARD_LVL_CODE    --客户卡级别_代码_借记卡
				,CARD_LVL         --客户卡级别_借记卡
				,CARD_DATE        --借记卡发卡日期
				,ETL_DATE         --跑批日期
				,DM_START_DT      --起始日期
				,DM_END_DT        --结束日期
			FROM '|| TMP_CUR ||  '  a
			WHERE
			NOT  EXISTS
			(
				SELECT
					 CUSTOMER_ID      --客户号
					,CIF_LVL_CODE     --客户级别_代码
					,CIF_LVL          --客户级别
					,CIF_LVL_DATE     --客户级别定级日期
					,CARD_LVL_CODE    --客户卡级别_代码_借记卡
					,CARD_LVL         --客户卡级别_借记卡
					,CARD_DATE        --借记卡发卡日期
				FROM '|| TMP_PRE ||  '  b
				WHERE	A.CUSTOMER_ID= B.CUSTOMER_ID
				AND	NVL(A.CIF_LVL_CODE,0)= NVL(B.CIF_LVL_CODE,0)
				AND	NVL(A.CARD_LVL_CODE,0)= NVL(B.CARD_LVL_CODE,0)
				AND	NVL(A.CARD_DATE,TO_DATE(99991231,''YYYYMMDD''))=NVL(B.CARD_DATE,TO_DATE(99991231,''YYYYMMDD''))
			)
		
		) A
		LEFT JOIN '|| TMP_PRE ||  '  C
		ON A.CUSTOMER_ID=C.CUSTOMER_ID';
		
		EXECUTE  IMMEDIATE DM_SQL;
		COMMIT;
		
		/*  pre-cur  = 删数据（包括修改前） */
		DM_SQL := 'INSERT INTO '|| TMP_UPD || '
		(
		 CUSTOMER_ID      --客户号
		,CIF_LVL_CODE     --客户级别_代码
		,CIF_LVL          --客户级别
		,CIF_LVL_DATE     --客户级别定级日期
		,CARD_LVL_CODE    --客户卡级别_代码_借记卡
		,CARD_LVL         --客户卡级别_借记卡
		,CARD_DATE        --借记卡发卡日期
		,ETL_DATE         --跑批日期
		,DM_START_DT      --起始日期
		,DM_END_DT        --结束日期
		)
		SELECT
			 CUSTOMER_ID      --客户号
			,CIF_LVL_CODE     --客户级别_代码
			,CIF_LVL          --客户级别
			,CIF_LVL_DATE     --客户级别定级日期
			,CARD_LVL_CODE    --客户卡级别_代码_借记卡
			,CARD_LVL         --客户卡级别_借记卡
			,CARD_DATE        --借记卡发卡日期
			,ETL_DATE         --跑批日期
			,DM_START_DT      --起始日期
			,DM_END_DT        --结束日期
		FROM '|| TMP_PRE ||' A
		WHERE
		NOT  EXISTS
		(
			SELECT
				 CUSTOMER_ID      --客户号
				,CIF_LVL_CODE     --客户级别_代码
				,CIF_LVL          --客户级别
				,CIF_LVL_DATE     --客户级别定级日期
				,CARD_LVL_CODE    --客户卡级别_代码_借记卡
				,CARD_LVL         --客户卡级别_借记卡
				,CARD_DATE        --借记卡发卡日期
				,ETL_DATE         --跑批日期
				,DM_START_DT      --起始日期
				,DM_END_DT        --结束日期
			FROM '|| TMP_CUR ||  '  B
			WHERE	A.CUSTOMER_ID= B.CUSTOMER_ID
			AND	NVL(A.CIF_LVL_CODE,0)= NVL(B.CIF_LVL_CODE,0)
			AND	NVL(A.CARD_LVL_CODE,0)= NVL(B.CARD_LVL_CODE,0)
			AND	NVL(A.CARD_DATE,TO_DATE(99991231,''YYYYMMDD''))=NVL(B.CARD_DATE,TO_DATE(99991231,''YYYYMMDD''))
		)';
		EXECUTE  IMMEDIATE DM_SQL;
		COMMIT;

		/* 更新历史中上日数据的截止时间为当日时间(删除-插入替代更新) */
		DM_SQL := 'DELETE FROM MMAPDM.DM_CUST_CARD_LVL_HIS A
		WHERE
		EXISTS(
			SELECT
				CUSTOMER_ID       --客户号
				,CIF_LVL_CODE     --客户级别_代码
				,CIF_LVL          --客户级别
				,CIF_LVL_DATE     --客户级别定级日期
				,CARD_LVL_CODE    --客户卡级别_代码_借记卡
				,CARD_LVL         --客户卡级别_借记卡
				,CARD_DATE        --借记卡发卡日期
				,ETL_DATE         --跑批日期
				,DM_START_DT      --起始日期
				,DM_END_DT        --结束日期
			FROM '|| TMP_UPD ||  ' B
			WHERE	A.CUSTOMER_ID= B.CUSTOMER_ID
			AND	NVL(A.CIF_LVL_CODE,0)= NVL(B.CIF_LVL_CODE,0)
			AND	NVL(A.CARD_LVL_CODE,0)= NVL(B.CARD_LVL_CODE,0)
			AND	NVL(A.CARD_DATE,TO_DATE(99991231,''YYYYMMDD''))=NVL(B.CARD_DATE,TO_DATE(99991231,''YYYYMMDD''))
			)
		AND	A.DM_END_DT='|| DM_MAX_DATE ;
		EXECUTE  IMMEDIATE DM_SQL;
		IO_ROW := SQL%ROWCOUNT ;


		DM_SQL := 'INSERT INTO MMAPDM.DM_CUST_CARD_LVL_HIS
		(
		CUSTOMER_ID       --客户号
		,CIF_LVL_CODE     --客户级别_代码
		,CIF_LVL          --客户级别
		,CIF_LVL_DATE     --客户级别定级日期
		,CARD_LVL_CODE    --客户卡级别_代码_借记卡
		,CARD_LVL         --客户卡级别_借记卡
		,CARD_DATE        --借记卡发卡日期
		,ETL_DATE         --跑批日期
		,DM_START_DT      --起始日期
		,DM_END_DT        --结束日期
		)
		SELECT
			CUSTOMER_ID       --客户号
			,CIF_LVL_CODE     --客户级别_代码
			,CIF_LVL          --客户级别
			,CIF_LVL_DATE     --客户级别定级日期
			,CARD_LVL_CODE    --客户卡级别_代码_借记卡
			,CARD_LVL         --客户卡级别_借记卡
			,CARD_DATE        --借记卡发卡日期
			,ETL_DATE         --跑批日期
			,DM_START_DT      --起始日期
			,'|| DM_TODAY ||'
		FROM '|| TMP_UPD ||  '';
		EXECUTE  IMMEDIATE DM_SQL;
		IO_ROW := IO_ROW+SQL%ROWCOUNT ;

		/* 插入新增数据  */
		DM_SQL := 'INSERT INTO  MMAPDM.DM_CUST_CARD_LVL_HIS
		(
		  CUSTOMER_ID       --客户号
		  ,CIF_LVL_CODE     --客户级别_代码
		  ,CIF_LVL          --客户级别
		  ,CIF_LVL_DATE     --客户级别定级日期
		  ,CARD_LVL_CODE    --客户卡级别_代码_借记卡
		  ,CARD_LVL         --客户卡级别_借记卡
		  ,CARD_DATE        --借记卡发卡日期
		  ,ETL_DATE         --跑批日期
		  ,DM_START_DT      --起始日期
		  ,DM_END_DT        --结束日期
		)
		SELECT
			CUSTOMER_ID       --客户号
			,CIF_LVL_CODE     --客户级别_代码
			,CIF_LVL          --客户级别
			,CIF_LVL_DATE     --客户级别定级日期
			,CARD_LVL_CODE    --客户卡级别_代码_借记卡
			,CARD_LVL         --客户卡级别_借记卡
			,CARD_DATE        --借记卡发卡日期
			,ETL_DATE         --跑批日期
			,DM_START_DT      --起始日期
			,DM_END_DT        --结束日期
		FROM '|| TMP_INS ||  '';
		EXECUTE  IMMEDIATE DM_SQL;
		IO_ROW := IO_ROW+SQL%ROWCOUNT ;
		
		P_MMAPDM_DROP_TMP_TABLES(TABLE_NAME,RETURN_NUM,RETURN_STATUS);
	END IF;
	
	/*写日志*/
	
	IO_STATUS := 0 ;
	IO_SQLERR := 'SUSSCESS';
	SELECT SYSDATE INTO  V_END_TIMESTAMP  FROM dual;  -- 加载程序运行结束时间
	P_MMAPDM_WRITE_LOGS(PROCEDURE_NAME,IO_STATUS,IO_ROW,V_START_TIMESTAMP,V_END_TIMESTAMP,IO_SQLERR);
	COMMIT;

	EXCEPTION
	  WHEN OTHERS THEN
	ROLLBACK ;
	IO_STATUS := 9 ;
	IO_SQLERR := SQLCODE ||  SQLERRM  ;
	SELECT SYSDATE INTO  V_END_TIMESTAMP  FROM dual;
	P_MMAPDM_WRITE_LOGS(PROCEDURE_NAME,IO_STATUS,IO_ROW,V_START_TIMESTAMP,V_END_TIMESTAMP,IO_SQLERR);
END  P_DM_CUST_CARD_LVL_HIS;
/
