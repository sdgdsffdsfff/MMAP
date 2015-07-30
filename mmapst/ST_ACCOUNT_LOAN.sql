/*
      客户贷款账户信息表 ST_ACCOUNT_LOAN
      author:lee5hx
      date:2015-07-30



*/
INSERT INTO ST_ACCOUNT_LOAN(
    ETL_DATE,   跑批日期跑批日期跑批日期跑批日期
    TX_DATE,   数据日期数据日期数据日期数据日期
    period_ID,   日期日期日期日期
    CUSTOMER_ID,   客户号客户号客户号客户号
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
    PRODTYP_ID,   
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
select
   to_number(to_char(SYSdate,'YYYYMMDD')),              --跑批日期
   da.TX_DT,                                            --数据日期
   to_number(to_char(da.TX_DT,'YYYYMMDD')),             --日期
   trim(da.CIF_Org),                                    --客户号
   da.Acc_Org,                                          --账户号
   da.Curr_ID,                                          --账户币种
   da.AccSts_ID,                                        --账户状态
   da.Brnh_ID,                                          --开户机构
   da.Deal_DT,                                          --开户日期
   da.Acc_Bal,                                          --账户余额
   da.AccTyp_Org,                                       --账户类型
   da.Offcr_ID,                                         --操作员
   da.StMaint_Dt,                                       --维护日期
   da.StMaint_Tm,                                       --维护时间
   da.MaintBuss_ID,                                     --维护交易代码
   da.ProdTyp_ID,                                       --产品类型（债券为投资类型 INVTYP_ID）
   da.Matur_Dt,                                         --到期日期
   da.Prod_Org,                                         --"产品大类1、资产类产品2、负债类产"
   da.Prod_No,                                          --产品代码
   da.PayStmtDis_No,                                    --对账单发送频率
   da.Value_DT,                                         --起息日
   da.PayStmtDis_ID,                                    --对账单发送方式
   da.Exch_Rate,                                        --汇率
   da.GLGrp_ID,                                         --核心总帐组
   da.N_Bal,                                            --积数
   da.Score_Bal,                                        --积分余额
   da.Mgmt_ID,                                          --管户人
   da.Int_Rate,                                         --贷款/透支利率
   da.BussTyp_ID,                                       --业务类型
   da.Invt_Org,                                         --投资组合号
   da.RATING_ID,                                        --债项评级
   da.SIC_ID,                                           --是否风险资产(呆滞、呆账）
   da.LNPurps_ID,                                       --贷款用途
   da.SETTAVGCOSTYST,                                   --摊余成本
   da.AApp_Org,                                         --综合申请号
   da.AAcc_Org,                                         --委托人结算账户
   da.ACIF_Org,                                         --委托人客户号
   da.BailAcc_Org,                                      --保证金账号
   da.DisAcc_Org,                                       --放款账号
   da.Car_ID,                                           --五级分类
   da.BusTyp_ID,                                        --行业分类
   da.LnTyp_ID,                                         --贷款类别
   da.MASInd_ID,                                        --贷款投向
   da.MthdCOL_ID,                                       --担保方式
   da.DISC_DT,                                          --银行折扣日期
   da.W_INSPRE1,                                        --保全费
   da.W_SUBSD,                                          --补贴金额
   da.TermTyp_ID,                                       --贷款期限代码
   da.Clos_DT,                                          --全部还款日
   da.FirstDis_DT,                                      --首次放款日
   da.FirstPay_DT,                                      --首次还款日
   da.FullDis_DT,                                       --全部放款日
   da.NDue_DT,                                          --偿还日
   da.Wo_DT,                                            --核销日期
   da.RatEffet_DT,                                      --利率生效日
   da.Pmt_ID,                                           --还款方式代码
   da.RatEfq_ID,                                        --利率频率代码
   da.RatEfqt_ID,                                       --利率频率
   da.IntFReq_ID,                                       --复利方式代码
   da.IntPrq_ID,                                        --利率偿还频率代码
   da.IntPFQT_ID,                                       --利率偿还频率类型
   da.YRBase_ID,                                        --利息计算方式
   da.Rat_Fee,                                          --手续费率
   da.Rat_LCHRG,                                        --罚息率
   da.Agnfee,                                           --委托人手续费
   da.PrnDis_Amt,                                       --已放款额
   da.Bail_Amt,                                         --保证金金额
   da.RPriceMthd_ID,                                    --利率重定价方法
   da.W_CBAL,                                           --本金余额
   da.LC_NO,                                            --信用证编号
   da.AccFeeCat_ID                                      --帐户年费/管理费代码
from MMAPST.DMMKT_ACASSET da
inner join MMAPST.DMMKT_CSMast db
on da.CIF_ORG = db.CIF_ORG
and trim(db.CIFTYP_ID) = 'I'
