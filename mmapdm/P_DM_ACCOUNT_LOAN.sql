CREATE OR REPLACE PROCEDURE MMAPDM.P_DM_ACCOUNT_LOAN
(IO_STATUS OUT INTEGER,IO_SQLERR OUT VARCHAR2)
AS
	TABLE_NAME VARCHAR2(125) :='DM_ACCOUNT_LOAN'; -- 表名
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
	SELECT COUNT(1) INTO ST_ROW FROM MMAPST.ST_ACCOUNT_LOAN WHERE PERIOD_ID=DM_TODAY;

	IF ST_ROW>0
	THEN
		/*针对同一天重复执行此存储过程的异常情况，插入前先删除"当日"数据*/
		DELETE FROM MMAPDM.DM_ACCOUNT_LOAN WHERE PERIOD_ID = DM_TODAY;    -- 删除"当日"数据
		COMMIT;





		/*插入当日交易数据*/
		DM_SQL := 'INSERT INTO MMAPDM.DM_ACCOUNT_LOAN(
				ETL_DATE,
				TX_DATE,
				period_ID,
				CUSTOMER_ID,
				ACCOUNT_ID,
				CURR_CODE,
				ACCT_STATUS,
				OPEN_BRANCH,
				OPEN_DATE,
				ACCT_BAL,
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
				Int_Rate,
				BussTyp_ID,
				Invt_Org,
				RATING_ID,
				SIC_ID,
				LNPurps_ID,
				SETTAVGCOSTYST,
				AApp_Org,
				AAcc_Org,
				ACUSTOMER_ID,
				BailAcc_Org,
				DisAcc_Org,
				Car_ID ,
				BusTyp_ID,
				LnTyp_ID,
				MASInd_ID,
				MthdCOL_ID,
				DISC_DT,
				W_INSPRE1,
				W_SUBSD,
				TermTyp_ID,
				Clos_DT,
				FirstDis_DT,
				FirstPay_DT,
				FullDis_DT,
				NDue_DT,
				Wo_DT,
				RatEffet_DT,
				Pmt_ID,
				RatEfq_ID,
				RatEfqt_ID,
				IntFReq_ID,
				IntPrq_ID,
				IntPFQT_ID,
				YRBase_ID,
				Rat_Fee,
				Rat_LCHRG,
				Agnfee,
				PrnDis_Amt,
				Bail_Amt,
				RPriceMthd_ID,
				W_CBAL,
				LC_NO,
				AccFeeCat_ID
			)
			SELECT
				'||V_ETL_DATE||',
				da.TX_DATE,
				da.period_ID,
				da.CUSTOMER_ID,
				da.ACCOUNT_ID,
				da.CURR_CODE,
				da.ACCT_STATUS,
				da.OPEN_BRANCH,
				da.OPEN_DATE,
				da.ACCT_BAL,
				da.ACCT_TYPE,
				da.Offcr_ID,
				da.StMaint_Dt,
				da.StMaint_Tm,
				da.MaintBuss_ID,
				(select PROD_TYPE_CODE from MMAPDM.DM_PRODUCT where PROD_CODE = da.PRODTYP_ID),
				da.END_DATE,
				da.Prod_Org,
				(select PROD_CODE from MMAPDM.DM_PRODUCT where PROD_CODE = da.PRODTYP_ID),
				da.PayStmtDis_No,
				da.Value_DT,
				da.PayStmtDis_ID,
				da.Exch_Rate,
				da.GLGrp_ID,
				da.N_Bal,
				da.Score_Bal,
				da.Mgmt_ID,
				da.Int_Rate,
				da.BussTyp_ID,
				da.Invt_Org,
				da.RATING_ID,
				da.SIC_ID,
				da.LNPurps_ID,
				da.SETTAVGCOSTYST,
				da.AApp_Org,
				da.AAcc_Org,
				da.ACUSTOMER_ID,
				da.BailAcc_Org,
				da.DisAcc_Org,
				da.Car_ID,
				da.BusTyp_ID,
				da.LnTyp_ID,
				da.MASInd_ID,
				da.MthdCOL_ID,
				da.DISC_DT,
				da.W_INSPRE1,
				da.W_SUBSD,
				da.TermTyp_ID,
				da.Clos_DT,
				da.FirstDis_DT,
				da.FirstPay_DT,
				da.FullDis_DT,
				da.NDue_DT,
				da.Wo_DT,
				da.RatEffet_DT,
				da.Pmt_ID,
				da.RatEfq_ID,
				da.RatEfqt_ID,
				da.IntFReq_ID,
				da.IntPrq_ID,
				da.IntPFQT_ID,
				da.YRBase_ID,
				da.Rat_Fee,
				da.Rat_LCHRG,
				da.Agnfee,
				da.PrnDis_Amt,
				da.Bail_Amt,
				da.RPriceMthd_ID,
				da.W_CBAL,
				da.LC_NO,
				da.AccFeeCat_ID
     		FROM MMAPST.ST_ACCOUNT_LOAN da';
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
END P_DM_ACCOUNT_LOAN;