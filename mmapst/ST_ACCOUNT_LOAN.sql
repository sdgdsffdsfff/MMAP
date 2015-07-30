/*
      客户贷款账户信息表 ST_ACCOUNT_LOAN
      author:lee5hx
      date:2015-07-30
*/
INSERT INTO ST_ACCOUNT_LOAN(
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
select
   to_number(to_char(SYSdate,'YYYYMMDD')),
   da.TX_DT,
   to_number(to_char(da.TX_DT,'YYYYMMDD')),
   trim(da.CIF_Org),
   trim(da.Acc_Org),
   trim(da.Curr_ID),
   da.AccSts_ID,
   da.Brnh_ID,
   da.Deal_DT,
   da.Acc_Bal,
   da.AccTyp_Org,
   da.Offcr_ID,
   da.StMaint_Dt,
   da.StMaint_Tm,
   da.MaintBuss_ID,
   da.ProdTyp_ID,
   da.Matur_Dt,
   da.Prod_Org,
   trim(da.Prod_No),
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
   da.ACIF_Org,
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
   trim(da.TermTyp_ID),
   da.Clos_DT,
   da.FirstDis_DT,
   da.FirstPay_DT,
   da.FullDis_DT,
   da.NDue_DT,
   da.Wo_DT,
   da.RatEffet_DT,
   trim(da.Pmt_ID),
   trim(da.RatEfq_ID),
   trim(da.RatEfqt_ID),
   trim(da.IntFReq_ID),
   trim(da.IntPrq_ID),
   trim(da.IntPFQT_ID),
   trim(da.YRBase_ID),
   da.Rat_Fee,
   da.Rat_LCHRG,
   da.Agnfee,
   da.PrnDis_Amt,
   da.Bail_Amt,
   trim(da.RPriceMthd_ID),
   da.W_CBAL,
   trim(da.LC_NO),
   trim(da.AccFeeCat_ID)
from MMAPST.DMMKT_ACASSET da
inner join MMAPST.DMMKT_CSMast db
on da.CIF_ORG = db.CIF_ORG
and trim(db.CIFTYP_ID) = 'I'
