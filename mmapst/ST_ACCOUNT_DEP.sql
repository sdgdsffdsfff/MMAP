/*
      客户存款类账户表 ST_ACCOUNT_DEP
      author:lee5hx
      date:2015-07-30
*/
INSERT INTO ST_ACCOUNT_DEP (
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
    Eff_Tm)
select
    to_number(to_char(SYSdate,'YYYYMMDD')),
    da.TX_DT,
    to_number(to_char(TX_DT,'YYYYMMDD')),
    trim(da.CIF_Org),
    da.Acc_Org,
    da.Curr_ID,
    da.AccSts_ID ,
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
    da.Prod_No,
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
from
    MMAPST.DMMKT_ACLIABI da
inner join MMAPST.DMMKT_CSMast db
on da.CIF_ORG = db.CIF_ORG
and trim(db.CIFTYP_ID) = 'I'
