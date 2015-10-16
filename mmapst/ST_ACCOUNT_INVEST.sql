/*
      客户投资类账户表 ST_ACCOUNT_INVEST
      author:lee5hx
      date:2015-09-23
      TestSQL:
        SELECT COUNT(*) FROM MMAPST.ST_ACCOUNT_INVEST
        DELETE FROM MMAPST.ST_ACCOUNT_INVEST
*/
TRUNCATE TABLE MMAPST.ST_ACCOUNT_INVEST;
INSERT INTO MMAPST.ST_ACCOUNT_INVEST (
        ETL_DATE,	                --跑批日期
        TX_DATE,                    --数据日期
        PERIOD_ID,	                --日期
        CUSTOMER_ID,	            --客户号
        ACCOUNT_ID,	                --资金账号（存款类账户号）
        ThirdPart_ACCOUNT_ID,	    --第三方账号（国债账号、基金、理财。。。）
        ACCT_TYPE,	                --账户类型
        ACCT_STATUS,	            --账户状态
        OPEN_BRANCH,	            --开户机构
        OPEN_DATE,	                --开户日期
        END_DATE,	                --到期日期
        PROD_CODE,	                --产品代码
        ACCT_BAL	                --账户余额
)
select
    to_number(to_char(SYSDATE,'YYYYMMDD')),
    a.TX_DT,
    to_number(to_char(a.TX_DT,'YYYYMMDD')),
    TRIM(a.CIF_ORG),
    TRIM(a.Acc_ORG),
    TRIM(a.THIRDPART_ORG),
    null,
    TRIM(A.AccSts_ID),
    TRIM(a.Brnh_ID),
    TRIM(a.EDeal_DT),
    null,
    TRIM(b.ProdTyp_ID),
    TRIM(b.Face_Amt)
 from
    MMAPDMMKT.DMMKT_ACEBond a INNER JOIN MMAPDMMKT.DMMKT_ACEBondAmt b
    ON a.THIRDPART_ORG=b.THIRDPART_ORG
    AND a.TX_DT=b.TX_DT
union all
 select
    to_number(to_char(SYSDATE,'YYYYMMDD')),
    c.TX_DT,
    to_number(to_char(c.TX_DT,'YYYYMMDD')),
    TRIM(c.CIF_ORG),
    TRIM(c.Acc_Org),
    TRIM(d.ThirdPart_Org),
    null,
    TRIM(d.AccSts_ID),
    TRIM(d.BRNH_ID),
    TRIM(d.Deal_DT),
    null,
    TRIM(c.ProdTyp_ID),
    TRIM(c.Vol_Amt)
 from
    MMAPDMMKT.DMMKT_ACFSShare c INNER JOIN MMAPDMMKT.DMMKT_ACFS d
    ON c.ThirdPart_Org=D.ThirdPart_Org
    AND c.CIF_Org=d.CIF_Org
    AND c.TX_DT=d.TX_DT

;
COMMIT;