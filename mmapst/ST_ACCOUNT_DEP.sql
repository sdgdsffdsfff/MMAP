/*
      客户存款类账户表 ST_ACCOUNT_DEP
      author:lee5hx
      date:2015-07-30
      TestSQL:
        SELECT COUNT(*) FROM MMAPST.ST_ACCOUNT_DEP
        DELETE FROM MMAPST.ST_ACCOUNT_DEP
*/
INSERT INTO MMAPST.ST_ACCOUNT_DEP (
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
    TRIM(DA.ACCTYP_ORG),
    TRIM(DA.OFFCR_ID),
    DA.STMAINT_DT,
    DA.STMAINT_TM,
    TRIM(DA.MAINTBUSS_ID),
    TRIM(DA.PRODTYP_ID),
    DA.MATUR_DT,
    TRIM(DA.PROD_ORG),
    TRIM(DA.PROD_NO),
    TRIM(DA.PAYSTMTDIS_NO),
    DA.VALUE_DT,
    TRIM(DA.PAYSTMTDIS_ID),
    DA.EXCH_RATE,
    TRIM(DA.GLGRP_ID),
    DA.N_BAL,
    DA.SCORE_BAL,
    TRIM(DA.MGMT_ID),
    TRIM(DA.CKTYP_ID),
    TRIM(DA.INTCAL_ID),
    DA.REDEMP_DT,
    TRIM(DA.YRBASE_ID),
    DA.RENEW_FG,
    TRIM(DA.WDISP_ID),
    DA.DY_AGGL,
    TRIM(DA.PMTFRQ_ID),
    TRIM(DA.RECEIPT_ID),
    TRIM(DA.TERMTYP_ID),
    DA.INT_RATE,
    DA.LEFTINT_AMT,
    DA.MTPRN_AMT,
    DA.TW_AMTCR,
    DA.TW_AMTDR,
    TRIM(DA.ACCGRP_ORG),
    TRIM(DA.HOLD_ID),
    TRIM(DA.TD_ID),
    DA.SCHED_AMT,
    DA.FXTYP_FG,
    DA.N_RENEW,
    DA.RPRICEFREQ,
    TRIM(DA.RPRICEFREQ_ID),
    DA.HOLD_AMT,
    DA.CRTACC_AMT,
    DA.PEN_AMT,
    TRIM(DA.THIRDPART_ORG),
    DA.EFF_DT,
    DA.EFF_TM
FROM
    MMAPST.DMMKT_ACLIABI DA
INNER JOIN MMAPST.DMMKT_CSMAST DB
ON DA.CIF_ORG = DB.CIF_ORG
AND TRIM(DB.CIFTYP_ID) = 'I'
INNER JOIN MMAPST.DMMKT_SYSTEM_DATE C --仅取"今日"数据
ON DA.TX_DT=TO_DATE(C.TX_DATE,'YYYYMMDD');



