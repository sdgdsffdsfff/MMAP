CREATE OR REPLACE PROCEDURE MMAPDM.P_DM_ACCOUNT_DEP
(IO_STATUS OUT INTEGER,IO_SQLERR OUT VARCHAR2)
AS
	TABLE_NAME VARCHAR2(125) :='DM_ACCOUNT_DEP'; -- 表名
	PROCEDURE_NAME VARCHAR2(125) :='P_'||TABLE_NAME;  -- 存储过程名(修改)

	DM_SQL VARCHAR2(20000); -- 存放SQL语句

	IO_ROW INTEGER;  --插入条数
	ST_ROW INTEGER;  --源数据条数

	V_ETL_DATE NUMBER;  -- 跑批日期
	V_START_TIMESTAMP TIMESTAMP;    -- 加载开始时间
	V_END_TIMESTAMP   TIMESTAMP;    -- 加载结束时间

	DM_TODAY NUMBER;        -- 数据日期"当日"
	TX_DATE NUMBER;

BEGIN
	SELECT SYSDATE INTO V_START_TIMESTAMP FROM dual;    -- 加载程序运行开始时间

	SELECT TO_NUMBER(TO_CHAR((SYSDATE),'YYYYMMDD')) INTO V_ETL_DATE FROM dual;  -- 取系统日期作为跑批日期
	SELECT TX_DATE INTO DM_TODAY FROM MMAPST.ST_SYSTEM_DATE;  -- 取数据日期

	--查询ST层表中是否有'当日'数据
	SELECT COUNT(1) INTO ST_ROW FROM MMAPST.ST_ACCOUNT_DEP WHERE PERIOD_ID=DM_TODAY;

	IF ST_ROW>0
	THEN
		/*针对同一天重复执行此存储过程的异常情况，插入前先删除"当日"数据*/
		DELETE FROM MMAPDM.DM_ACCOUNT_DEP WHERE PERIOD_ID = DM_TODAY;    -- 删除"当日"数据
		COMMIT;





		/*插入当日交易数据*/
		DM_SQL := 'INSERT INTO MMAPDM.DM_ACCOUNT_DEP(
				ETL_DATE,
				TX_DATE,
				period_ID,
				CUSTOMER_ID,
				ACCOUNT_ID,
				CURR_CODE,
				ACCT_STATUS_CODE,
				OPEN_BRANCH,
				OPEN_DATE,
				ACCT_Bal,
				ACCT_TYPE,
				Offcr_ID,
				StMaint_Dt,
				StMaint_Tm,
				MaintBuss_ID,
				ProdTyp_ID,
				END_DATE,
				Prod_Org,
				PROD_CODE,
				PayStmtDis_No,
				Value_DT,
				PayStmtDis_ID,
				Exch_Rate,
				GLGrp_ID,
				N_Bal,
				Score_Bal,
				Mgmt_ID,
				CkTyp_ID,
				IntCal_ID,
				ReDemp_DT,
				YrBase_ID,
				Renew_FG,
				WDisp_ID,
				Dy_AGGL,
				PMTFRQ_ID,
				Receipt_ID,
				TermTYP_ID,
				Int_Rate,
				LeftInt_Amt,
				MTPRN_Amt,
				TW_AMTCR,
				TW_AMTDR,
				AccGrp_ORG,
				Hold_ID,
				TD_ID,
				Sched_Amt,
				FxTyp_Fg,
				N_RENEW,
				RPriceFreq,
				RPriceFreq_ID,
				Hold_Amt,
				CrtAcc_Amt,
				Pen_Amt,
				thirdPart_Org,
				Eff_DT,
				Eff_Tm

			)
			SELECT
				'||V_ETL_DATE||',
				da.TX_DATE,
				da.period_ID,
				da.CUSTOMER_ID,
				da.ACCOUNT_ID,
				da.CURR_CODE,
				da.ACCT_STATUS_CODE,
				da.OPEN_BRANCH,
				da.OPEN_DATE,
				da.ACCT_Bal,
				da.ACCT_TYPE,
				da.Offcr_ID,
				da.StMaint_Dt,
				da.StMaint_Tm,
				da.MaintBuss_ID,
				da.ProdTyp_ID,
				da.END_DATE,
				da.Prod_Org,
				da.PROD_CODE,
				da.PayStmtDis_No,
				da.Value_DT,
				da.PayStmtDis_ID,
				da.Exch_Rate,
				da.GLGrp_ID,
				da.N_Bal,
				da.Score_Bal,
				da.Mgmt_ID,
				da.CkTyp_ID,
				da.IntCal_ID,
				da.ReDemp_DT,
				da.YrBase_ID,
				da.Renew_FG,
				da.WDisp_ID,
				da.Dy_AGGL,
				da.PMTFRQ_ID,
				da.Receipt_ID,
				da.TermTYP_ID,
				da.Int_Rate,
				da.LeftInt_Amt,
				da.MTPRN_Amt,
				da.TW_AMTCR,
				da.TW_AMTDR,
				da.AccGrp_ORG,
				da.Hold_ID,
				da.TD_ID,
				da.Sched_Amt,
				da.FxTyp_Fg,
				da.N_RENEW,
				da.RPriceFreq,
				da.RPriceFreq_ID,
				da.Hold_Amt,
				da.CrtAcc_Amt,
				da.Pen_Amt,
				da.thirdPart_Org,
				da.Eff_DT,
				da.Eff_TM
     		FROM MMAPST.ST_ACCOUNT_DEP da';

		EXECUTE IMMEDIATE DM_SQL;
		IO_ROW := SQL%ROWCOUNT ;
		COMMIT;
	END IF;






	/*写入日志信息*/
	SELECT SYSDATE INTO V_END_TIMESTAMP   FROM dual;    -- 加载程序运行结束时间
	IO_STATUS := 0 ;
	IO_SQLERR := 'SUSSCESS';
	P_MMAPDM_WRITE_LOGS(PROCEDURE_NAME,IO_STATUS,IO_ROW,V_START_TIMESTAMP,V_END_TIMESTAMP,IO_SQLERR);
	COMMIT;
	EXCEPTION
	  WHEN OTHERS THEN
	ROLLBACK ;
	IO_STATUS := 9 ;
	IO_SQLERR := SQLCODE || SQLERRM ;
	SELECT SYSDATE INTO V_END_TIMESTAMP FROM dual;
	P_MMAPDM_WRITE_LOGS(PROCEDURE_NAME,IO_STATUS,IO_ROW,V_START_TIMESTAMP,V_END_TIMESTAMP,IO_SQLERR);
END P_DM_ACCOUNT_DEP;