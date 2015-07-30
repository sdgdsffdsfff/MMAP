/*
      客户存款类账户表 ST_ACCOUNT_DEP
      author:lee5hx
      date:2015-07-30
*/
INSERT INTO ST_ACCOUNT_DEP (
    ETL_DATE,               --跑批日期
    TX_DATE,                --数据日期
    period_ID,              --日期
    CUSTOMER_ID,            --客户号
    ACCOUNT_ID,             --账户号（活期是资金账号，定期是协议号）
    CURR_CODE,              --账户币种
    ACCT_STATUS_CODE,       --账户状态
    OPEN_BRANCH,            --开户行号
    OPEN_DATE,              --开户日期
    ACCT_Bal,               --账户余额
    ACCT_TYPE,              --账户类型
    Offcr_ID,               --操作员
    StMaint_Dt,             --维护日期
    StMaint_Tm,             --维护时间
    MaintBuss_ID,           --维护交易代码
    ProdTyp_ID,             --产品类型债券为投资类型 INVTYP_ID
    END_DATE,               --到期日
    Prod_Org,               --"产品大类1、资产类产品2、负债类产品3、表外产品"
    PROD_CODE,              --产品代码
    PayStmtDis_No,          --对账单发送频率
    Value_DT,               --起息日
    PayStmtDis_ID,          --对账单发送方式
    Exch_Rate,              --汇率
    GLGrp_ID,               --核心总帐组
    N_Bal,                  --积数
    Score_Bal,              --积分余额
    Mgmt_ID,                --管户人
    CkTyp_ID,               --挂失状态
    IntCal_ID,              --利息计算方式
    ReDemp_DT,              --销户日期
    YrBase_ID,              --复利方式代码
    Renew_FG,               --自动转存标志（定期）
    WDisp_ID,               --转存状态（定期）
    Dy_AGGL,                --至今积数日
    PMTFRQ_ID,              --利息支付方式
    Receipt_ID,             --凭证号
    TermTYP_ID,             --期限
    Int_Rate,               --利率
    LeftInt_Amt,            --定期存款到期未提取利息
    MTPRN_Amt,              --定期存款到期未提取本金
    TW_AMTCR,               --当日累计存入（本地币种）
    TW_AMTDR,               --当日累计支取（本地币种）
    AccGrp_ORG,             --主账号
    Hold_ID,                --抑制代码
    TD_ID,                  --定期存单号
    Sched_Amt,              --计划本金（零存整取账户计划存入本）
    FxTyp_Fg,               --钞汇标志
    N_RENEW,                --转存次数
    RPriceFreq,             --重定价频率
    RPriceFreq_ID,          --重定价频率单位
    Hold_Amt,               --冻结金额
    CrtAcc_Amt,             --开户金额（定期）
    Pen_Amt,                --罚息额（本地币种）
    thirdPart_Org,          --第三方账号
    Eff_DT,                 --生效日期
    Eff_Tm                  --生效时间
    )
select
    to_number(to_char(SYSdate,'YYYYMMDD')),
    da.TX_DT,
    to_number(to_char(TX_DT,'YYYYMMDD')),
    trim(da.CIF_Org),
    trim(da.Acc_Org),
    trim(da.Curr_ID),
    da.AccSts_ID ,
    da.Brnh_ID,
    da.Deal_DT,
    da.Acc_Bal,
    da.AccTyp_Org,
    da.Offcr_ID,
    da.StMaint_Dt,
    da.StMaint_Tm,
    trim(da.MaintBuss_ID),
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
    da.CkTyp_ID,
    da.IntCal_ID,
    da.ReDemp_DT,
    trim(da.YrBase_ID),
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
    trim(da.thirdPart_Org),
    da.Eff_DT,
    da.Eff_TM
from
    MMAPST.DMMKT_ACLIABI da
inner join MMAPST.DMMKT_CSMast db
on da.CIF_ORG = db.CIF_ORG
and trim(db.CIFTYP_ID) = 'I'



