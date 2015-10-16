/*
      客户贷款账户信息表 ST_ACCOUNT_LOAN
      author:lee5hx
      date:2015-07-30
      TestSQL:
        SELECT COUNT(*) FROM MMAPST.ST_ACCOUNT_LOAN
        DELETE FROM MMAPST.ST_ACCOUNT_LOAN
*/
TRUNCATE TABLE MMAPST.ST_ACCOUNT_LOAN;
INSERT INTO MMAPST.ST_ACCOUNT_LOAN(
    ETL_DATE,                   --跑批日期
    TX_DATE,                    --数据日期
    period_ID,                  --日期
    CUSTOMER_ID,                --客户号
    ACCOUNT_ID,                 --账户号
    CURR_CODE,                  --账户币种
    ACCT_STATUS,                --账户状态
    OPEN_BRANCH,                --开户机构
    OPEN_DATE,                  --开户日期
    ACCT_BAL,                   --账户余额
    ACCT_TYPE,                  --账户类型
    Offcr_ID,                   --操作员
    StMaint_Dt,                 --维护日期
    StMaint_Tm,                 --维护时间
    MaintBuss_ID,               --维护交易代码
    PRODTYP_ID,                 --产品类型（债券为投资类型 INVTYP_ID）
    END_DATE,                   --到期日期
    Prod_Org,                   --"产品大类1、资产类产品2、负债类产"
    PROD_CODE,                  --产品代码
    PayStmtDis_No,              --对账单发送频率
    Value_DT,                   --起息日
    PayStmtDis_ID,              --对账单发送方式
    Exch_Rate,                  --汇率
    GLGrp_ID,                   --核心总帐组
    N_Bal,                      --积数
    Score_Bal,                  --积分余额
    Mgmt_ID,                    --管户人
    Int_Rate,                   --贷款/透支利率
    BussTyp_ID,                 --业务类型
    Invt_Org,                   --投资组合号
    RATING_ID,                  --债项评级
    SIC_ID,                     --是否风险资产(呆滞、呆账）
    LNPurps_ID,                 --贷款用途
    SETTAVGCOSTYST,             --摊余成本
    AApp_Org,                   --综合申请号
    AAcc_Org,                   --委托人结算账户
    ACUSTOMER_ID,               --委托人客户号
    BailAcc_Org,                --保证金账号
    DisAcc_Org,                 --放款账号
    Car_ID ,                    --五级分类
    BusTyp_ID,                  --行业分类
    LnTyp_ID,                   --贷款类别
    MASInd_ID,                  --贷款投向
    MthdCOL_ID,                 --担保方式
    DISC_DT,                    --银行折扣日期
    W_INSPRE1,                  --保全费
    W_SUBSD,                    --补贴金额
    TermTyp_ID,                 --贷款期限代码
    Clos_DT,                    --全部还款日
    FirstDis_DT,                --首次放款日
    FirstPay_DT,                --首次还款日
    FullDis_DT,                 --全部放款日
    NDue_DT,                    --偿还日
    Wo_DT,                      --核销日期
    RatEffet_DT,                --利率生效日
    Pmt_ID,                     --还款方式代码
    RatEfq_ID,                  --利率频率代码
    RatEfqt_ID,                 --利率频率
    IntFReq_ID,                 --复利方式代码
    IntPrq_ID,                  --利率偿还频率代码
    IntPFQT_ID,                 --利率偿还频率类型
    YRBase_ID,                  --利息计算方式
    Rat_Fee,                    --手续费率
    Rat_LCHRG,                  --罚息率
    Agnfee,                     --委托人手续费
    PrnDis_Amt,                 --已放款额
    Bail_Amt,                   --保证金金额
    RPriceMthd_ID,              --利率重定价方法
    W_CBAL,                     --本金余额
    LC_NO,                      --信用证编号
    AccFeeCat_ID                --帐户年费/管理费代码
)
SELECT
   TO_NUMBER(TO_CHAR(SYSDATE,'YYYYMMDD')),
   DA.TX_DT,
   TO_NUMBER(TO_CHAR(DA.TX_DT,'YYYYMMDD')),
   TRIM(DA.CIF_ORG),
   TRIM(DA.ACC_ORG),
   TRIM(DA.CURR_ID),
   TRIM(DA.ACCSTS_ID),
   TRIM(DA.BRNH_ID),
   DA.DEAL_DT,
   DA.ACC_BAL,
   DA.ACCTYP_ORG,
   TRIM(DA.OFFCR_ID),
   DA.STMAINT_DT,
   DA.STMAINT_TM,
   TRIM(DA.MAINTBUSS_ID),
   TRIM(DA.PRODTYP_ID),
   DA.MATUR_DT,
   DA.PROD_ORG,
   TRIM(DA.PROD_NO),
   DA.PAYSTMTDIS_NO,
   DA.VALUE_DT,
   TRIM(DA.PAYSTMTDIS_ID),
   DA.EXCH_RATE,
   TRIM(DA.GLGRP_ID),
   DA.N_BAL,
   DA.SCORE_BAL,
   TRIM(DA.MGMT_ID),
   DA.INT_RATE,
   TRIM(DA.BUSSTYP_ID),
   DA.INVT_ORG,
   TRIM(DA.RATING_ID),
   TRIM(DA.SIC_ID),
   TRIM(DA.LNPURPS_ID),
   TRIM(DA.SETTAVGCOSTYST),
   TRIM(DA.AAPP_ORG),
   TRIM(DA.AACC_ORG),
   TRIM(DA.ACIF_ORG),
   TRIM(DA.BAILACC_ORG),
   TRIM(DA.DISACC_ORG),
   TRIM(DA.CAR_ID),
   TRIM(DA.BUSTYP_ID),
   TRIM(DA.LNTYP_ID),
   TRIM(DA.MASIND_ID),
   TRIM(DA.MTHDCOL_ID),
   DA.DISC_DT,
   DA.W_INSPRE1,
   DA.W_SUBSD,
   TRIM(DA.TERMTYP_ID),
   DA.CLOS_DT,
   DA.FIRSTDIS_DT,
   DA.FIRSTPAY_DT,
   DA.FULLDIS_DT,
   DA.NDUE_DT,
   DA.WO_DT,
   DA.RATEFFET_DT,
   TRIM(DA.PMT_ID),
   TRIM(DA.RATEFQ_ID),
   TRIM(DA.RATEFQT_ID),
   TRIM(DA.INTFREQ_ID),
   TRIM(DA.INTPRQ_ID),
   TRIM(DA.INTPFQT_ID),
   TRIM(DA.YRBASE_ID),
   DA.RAT_FEE,
   DA.RAT_LCHRG,
   DA.AGNFEE,
   DA.PRNDIS_AMT,
   DA.BAIL_AMT,
   TRIM(DA.RPRICEMTHD_ID),
   DA.W_CBAL,
   TRIM(DA.LC_NO),
   TRIM(DA.ACCFEECAT_ID)
FROM MMAPDMMKT.DMMKT_ACASSET DA
INNER JOIN MMAPDMMKT.DMMKT_CSMAST DB
ON DA.CIF_ORG = DB.CIF_ORG
AND TRIM(DB.CIFTYP_ID) = 'I'
INNER JOIN MMAPDMMKT.DMMKT_SYSTEM_DATE C --仅取"今日"数据
ON DA.TX_DT=TO_DATE(C.TX_DATE,'YYYYMMDD');

COMMIT;