CREATE OR REPLACE PROCEDURE "MMAPDM"."P_MID_ITEM"
AS
/*
truncate table MMAPDM.MID_ITEM;
INSERT INTO MMAPDM.MID_ITEM
(
source_item_id
,source_item_nm
,source_item_typ
,source_item_des
,item_id
,item_nm
,item_typ
,item_des
,vaule_char
,value_num
)
--客户风险等级_代码
select distinct	TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,TRIM(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN '未知'  ELSE TRIM(a.item_nm) END
       ,'RISK_LVL'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  MMAPST.ST_ITEM a
WHERE ITEM_TYP ='Risk_Lvl'
UNION ALL
--客户级别
select distinct  TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,CASE trim(a.item_id)
       WHEN '00' THEN 'PK'
       WHEN '01' THEN 'JK'
       WHEN '50' THEN 'JK'
       WHEN '55' THEN 'BJK'
       WHEN '60' THEN 'ZSK'
       WHEN '70' THEN 'CFK'
       WHEN '80' THEN 'SRYH'
   ELSE '-'
   END
       ,CASE trim(a.item_id)
       WHEN '00' THEN '普卡'
       WHEN '01' THEN '金卡'
       WHEN '50' THEN '金卡'
       WHEN '55' THEN '白金卡'
       WHEN '60' THEN '钻石卡'
       WHEN '70' THEN '财富卡'
       WHEN '80' THEN '私人银行'
   ELSE '未知'
   END
       ,'CIF_LVL'
       ,TRIM(a.item_des)
       ,NULL
       ,CASE trim(a.item_id)
       WHEN '00' THEN 10
       WHEN '01' THEN 20
       WHEN '50' THEN 20
       WHEN '55' THEN 30
       WHEN '60' THEN 40
       WHEN '70' THEN 50
       WHEN '80' THEN 60
   ELSE 0
   END
from  MMAPST.ST_ITEM a
WHERE ITEM_TYP ='CIF_Lvl'
UNION ALL
--民族
select distinct  TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,TRIM(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN '未知'  ELSE TRIM(a.item_nm) END
       ,'RACE'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  MMAPST.ST_ITEM a
WHERE ITEM_TYP ='RaceTyp_ID'
UNION ALL
--国籍
select distinct  TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,trim(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN '未知'  ELSE TRIM(a.item_nm) END
       ,'COUNTRY'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  MMAPST.ST_ITEM a
WHERE ITEM_TYP ='Citizen_ID'
UNION ALL
--职称
select distinct  TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,trim(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN '未知'  ELSE TRIM(a.item_nm) END
       ,'POSITION_TITLE'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  MMAPST.ST_ITEM a
WHERE ITEM_TYP ='ProfTitl_ID'
UNION ALL
--职业
select distinct  TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,trim(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN '未知'  ELSE TRIM(a.item_nm) END
       ,'OCCP'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  MMAPST.ST_ITEM a
WHERE ITEM_TYP ='Occp_ID'
UNION ALL
--职位
select distinct  TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,trim(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN '未知'  ELSE TRIM(a.item_nm) END
       ,'POSITION'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  MMAPST.ST_ITEM a
WHERE ITEM_TYP ='JobDes_ID'
UNION ALL
--婚姻状态
select distinct  TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,TRIM(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN '未知'  ELSE TRIM(a.item_nm) END
       ,'MARITALSTS'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  MMAPST.ST_ITEM a
WHERE ITEM_TYP ='MaritalSts_ID'
UNION ALL
--教育程度
select distinct  TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,trim(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN '未知'  ELSE TRIM(a.item_nm) END
       ,'EDU'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  MMAPST.ST_ITEM a
WHERE ITEM_TYP ='Edu_ID'
UNION ALL
--性别
select distinct  TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,TRIM(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN '未知'  ELSE TRIM(a.item_nm) END
       ,'GENDER'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  MMAPST.ST_ITEM a
WHERE ITEM_TYP ='Gender_ID'

--总账代码
UNION ALL
select distinct  TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,trim(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN '未知'  ELSE TRIM(a.item_nm) END
       ,'GLGRP_ID'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  MMAPST.ST_ITEM A
where item_typ='GLGrp_ID'
and bak3='PDAsset'
--借记卡类别
UNION ALL
select distinct  TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,trim(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN '未知'  ELSE TRIM(a.item_nm) END
       ,'CRDTYP'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  MMAPST.ST_ITEM A
where item_typ='CrdTyp_ID'
--帐户状态
UNION ALL
<<<<<<< HEAD:mmapdm/MID_ITEM.sql
select distinct	TRIM(a.item_id)
=======
select distinct  TRIM(a.item_id)
>>>>>>> origin/Test:mmapdm/P_MID_ITEM.sql
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,trim(a.item_id)
<<<<<<< HEAD:mmapdm/MID_ITEM.sql
       ,CASE WHEN trim(a.item_id)='-' THEN '未知'	ELSE TRIM(a.item_nm) END
=======
       ,CASE WHEN trim(a.item_id)='-' THEN '未知'  ELSE TRIM(a.item_nm) END
>>>>>>> origin/Test:mmapdm/P_MID_ITEM.sql
       ,'ACCT_STATUS'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
<<<<<<< HEAD:mmapdm/MID_ITEM.sql
from  MMAPST.Dmmkt_Pcodemast A
=======
from  MMAPST.ST_ITEM A
>>>>>>> origin/Test:mmapdm/P_MID_ITEM.sql
where item_typ='AccSts_ID'
;
COMMIT;
--借记卡级别
INSERT INTO MMAPDM.MID_ITEM VALUES('-','','CrdTyp_ID','卡类型','WZ','未知','CARD_LVL','借记卡级别','','0');
INSERT INTO MMAPDM.MID_ITEM VALUES('EBON','理财卡','CrdTyp_ID','卡类型','PK','普卡','CARD_LVL','借记卡级别','','10');
INSERT INTO MMAPDM.MID_ITEM VALUES('LTMC','海融联通卡','CrdTyp_ID','卡类型','PK','普卡','CARD_LVL','借记卡级别','','10');
INSERT INTO MMAPDM.MID_ITEM VALUES('NORI','新普通卡','CrdTyp_ID','卡类型','PK','普卡','CARD_LVL','借记卡级别','','10');
INSERT INTO MMAPDM.MID_ITEM VALUES('NORM','普通卡','CrdTyp_ID','卡类型','PK','普卡','CARD_LVL','借记卡级别','','10');
INSERT INTO MMAPDM.MID_ITEM VALUES('LQNC','利群普通卡','CrdTyp_ID','卡类型','PK','普卡','CARD_LVL','借记卡级别','','10');
INSERT INTO MMAPDM.MID_ITEM VALUES('QQCC','海融青春卡','CrdTyp_ID','卡类型','PK','普卡','CARD_LVL','借记卡级别','','10');
INSERT INTO MMAPDM.MID_ITEM VALUES('QQDC','海融青大卡','CrdTyp_ID','卡类型','PK','普卡','CARD_LVL','借记卡级别','','10');
INSERT INTO MMAPDM.MID_ITEM VALUES('SIXE','6+E卡','CrdTyp_ID','卡类型','PK','普卡','CARD_LVL','借记卡级别','','10');
INSERT INTO MMAPDM.MID_ITEM VALUES('SOCS','社保金融卡','CrdTyp_ID','卡类型','PK','普卡','CARD_LVL','借记卡级别','','10');
INSERT INTO MMAPDM.MID_ITEM VALUES('SSYC','世园卡','CrdTyp_ID','卡类型','PK','普卡','CARD_LVL','借记卡级别','','10');
INSERT INTO MMAPDM.MID_ITEM VALUES('TSMC','TSM电子现金卡','CrdTyp_ID','卡类型','PK','普卡','CARD_LVL','借记卡级别','','10');
INSERT INTO MMAPDM.MID_ITEM VALUES('VCRD','联名卡','CrdTyp_ID','卡类型','PK','普卡','CARD_LVL','借记卡级别','','10');
INSERT INTO MMAPDM.MID_ITEM VALUES('WCRD','微尘卡','CrdTyp_ID','卡类型','PK','普卡','CARD_LVL','借记卡级别','','10');
INSERT INTO MMAPDM.MID_ITEM VALUES('YKTC','海融普通卡','CrdTyp_ID','卡类型','PK','普卡','CARD_LVL','借记卡级别','','10');
INSERT INTO MMAPDM.MID_ITEM VALUES('NOGC','金卡','CrdTyp_ID','卡类型','JK','金卡','CARD_LVL','借记卡级别','','20');
INSERT INTO MMAPDM.MID_ITEM VALUES('YKTG','海融金卡','CrdTyp_ID','卡类型','JK','金卡','CARD_LVL','借记卡级别','','20');
INSERT INTO MMAPDM.MID_ITEM VALUES('LQGC','利群金卡','CrdTyp_ID','卡类型','JK','金卡','CARD_LVL','借记卡级别','','20');
INSERT INTO MMAPDM.MID_ITEM VALUES('YKTP','海融白金卡','CrdTyp_ID','卡类型','BJK','白金卡','CARD_LVL','借记卡级别','','30');
INSERT INTO MMAPDM.MID_ITEM VALUES('LQPC','利群白金卡','CrdTyp_ID','卡类型','BJK','白金卡','CARD_LVL','借记卡级别','','30');
INSERT INTO MMAPDM.MID_ITEM VALUES('NOPC','白金卡','CrdTyp_ID','卡类型','BJK','白金卡','CARD_LVL','借记卡级别','','30');
INSERT INTO MMAPDM.MID_ITEM VALUES('DCRD','海融钻石卡','CrdTyp_ID','卡类型','ZSK','钻石卡','CARD_LVL','借记卡级别','','40');
INSERT INTO MMAPDM.MID_ITEM VALUES('CCRD','海融财富卡','CrdTyp_ID','卡类型','CFK','财富卡','CARD_LVL','借记卡级别','','50');
INSERT INTO MMAPDM.MID_ITEM VALUES('PVBC','海融私人银行卡','CrdTyp_ID','卡类型','SRYH','私人银行','CARD_LVL','借记卡级别','','60');


--客户反馈代码
INSERT INTO MMAPDM.MID_ITEM VALUES('10001','完成营销/任务(关怀,满意度调查..),不需转介','RESPONSE','客户反馈代码','10001','愿意购买','RESPONSE','客户反馈信息','意愿明确，愿意购买','10000');
INSERT INTO MMAPDM.MID_ITEM VALUES('10002','','RESPONSE','客户反馈代码','10002','','RESPONSE','客户反馈信息','完成营销,需转介（批量）','10000');
INSERT INTO MMAPDM.MID_ITEM VALUES('10003','','RESPONSE','客户反馈代码','10003','','RESPONSE','客户反馈信息','完成营销,需转介（实时）','10000');
INSERT INTO MMAPDM.MID_ITEM VALUES('10004','同渠道页面跳转或点击超链接','RESPONSE','客户反馈代码','10004','客户有意愿，直接通过营销页面跳转至交易页面。','RESPONSE','客户反馈信息','有意愿，电子渠道页面跳转','10000');
INSERT INTO MMAPDM.MID_ITEM VALUES('10005','有意愿购买，愿意再深入、详细了解','RESPONSE','客户反馈代码','10005','有意愿购买，愿意再深入、详细了解','RESPONSE','客户反馈信息','有意愿购买，愿意再深入、详细了解','10000');
INSERT INTO MMAPDM.MID_ITEM VALUES('10006','有购买意愿，但目前资金不到位','RESPONSE','客户反馈代码','20001','有购买意愿，但目前资金不到位','RESPONSE','客户反馈信息','有购买意愿，但目前资金不到位','20000');
INSERT INTO MMAPDM.MID_ITEM VALUES('10999','其他','RESPONSE','客户反馈代码','20002','其他','RESPONSE','客户反馈信息','有意愿-其他','20000');
INSERT INTO MMAPDM.MID_ITEM VALUES('20001','产品不符合客户需求--产品起点过高','RESPONSE','客户反馈代码','20003','产品不符合客户需求--产品起点过高','RESPONSE','客户反馈信息','产品不符合(客户)需求--产品起点过高','20000');
INSERT INTO MMAPDM.MID_ITEM VALUES('20002','产品不符合客户需求--客户偏好定期类稳健产品','RESPONSE','客户反馈代码','20004','产品不符合客户需求--客户偏好定期类稳健产品','RESPONSE','客户反馈信息','产品不符合客户需求--客户偏好定期类稳健产品','20000');
INSERT INTO MMAPDM.MID_ITEM VALUES('20003','产品不符合客户需求--客户偏好股市、基金等风险投资','RESPONSE','客户反馈代码','20005','产品不符合客户需求--客户偏好股市、基金等风险投资','RESPONSE','客户反馈信息','产品不符合客户需求--客户偏好股市、基金等风险投资','20000');
INSERT INTO MMAPDM.MID_ITEM VALUES('20004','产品不符合客户需求--其他','RESPONSE','客户反馈代码','20006','产品不符合客户需求--其他','RESPONSE','客户反馈信息','产品不符合客户需求--其他','20000');
INSERT INTO MMAPDM.MID_ITEM VALUES('20005','产品不符合(客户)需求','RESPONSE','客户反馈代码','20007','该产品不能满足客户具体的某一项需求','RESPONSE','客户反馈信息','无意愿，产品不能满足需求','20000');
INSERT INTO MMAPDM.MID_ITEM VALUES('20006','需深入营销','RESPONSE','客户反馈代码','20999','需深入营销','RESPONSE','客户反馈信息','需深入营销','20000');
INSERT INTO MMAPDM.MID_ITEM VALUES('20007','客户表示没兴趣','RESPONSE','客户反馈代码','30001','客户根本不需要该类产品','RESPONSE','客户反馈信息','无意愿，对产品不感兴趣','30000');
INSERT INTO MMAPDM.MID_ITEM VALUES('20999','其他(请说明)','RESPONSE','客户反馈代码','60001','客户未应答，拒绝告知原因','RESPONSE','客户反馈信息','无意愿，拒绝告知原因','60000');
INSERT INTO MMAPDM.MID_ITEM VALUES('30001','拒绝营销','RESPONSE','客户反馈代码','60002','“不要给我打电话”','RESPONSE','客户反馈信息','拒绝营销，不希望再被骚扰','60000');
INSERT INTO MMAPDM.MID_ITEM VALUES('60001','电话错误/空号','RESPONSE','客户反馈代码','60003','您拨的号码有误或该电话是空号','RESPONSE','客户反馈信息','无效号码或空号','60000');
INSERT INTO MMAPDM.MID_ITEM VALUES('60002','更换电话/无此人','RESPONSE','客户反馈代码','60004','接电话非客户本人，并且与客户不曾相识','RESPONSE','客户反馈信息','错误号码，非客户本人','60000');
INSERT INTO MMAPDM.MID_ITEM VALUES('60003','无人接听(达一定次数以上)','RESPONSE','客户反馈代码','60999','电话接通，但无人接听','RESPONSE','客户反馈信息','多次无人接听','60000');
INSERT INTO MMAPDM.MID_ITEM VALUES('60005','E-mail错误(寄送失败）','RESPONSE','客户反馈代码','99999','E-mail被退信，无法送达目标地址','RESPONSE','客户反馈信息','错误电邮地址','60000');
INSERT INTO MMAPDM.MID_ITEM VALUES('60999','其他','RESPONSE','客户反馈代码','80001','其他','RESPONSE','客户反馈信息','其他','80000');
INSERT INTO MMAPDM.MID_ITEM VALUES('99999','无任何反应','RESPONSE','客户反馈代码','80002','非电话渠道，比如短信、网银，客户无视营销信息，没有任何反馈。','RESPONSE','客户反馈信息','客户无任何反馈','80000');
INSERT INTO MMAPDM.MID_ITEM VALUES('80001','考虑中，待追踪','RESPONSE','客户反馈代码','80003','客户对产品表示关心，没有明确意愿，需要再次回拨电话持续沟通','RESPONSE','客户反馈信息','考虑中，待追踪','80000');
INSERT INTO MMAPDM.MID_ITEM VALUES('80002','不方便接听','RESPONSE','客户反馈代码','80004','无关于产品的销售对话，由于会议中或其它特殊情况，客户不方便接听','RESPONSE','客户反馈信息','不方便接听','80000');
INSERT INTO MMAPDM.MID_ITEM VALUES('80003','不在(非本人接听)','RESPONSE','客户反馈代码','80999','客户电话无误，但并非本人接听，需要择时再回拨联系本人。','RESPONSE','客户反馈信息','不在(非本人接听)','80000');
INSERT INTO MMAPDM.MID_ITEM VALUES('80004','非决策者,需联系他人','RESPONSE','客户反馈代码','90000','客户本人不能单独做决定，需要与其他人商议再做购买决定','RESPONSE','客户反馈信息','非决策者,需联系他人','90000');
INSERT INTO MMAPDM.MID_ITEM VALUES('80999','其他','RESPONSE','客户反馈代码','91000','其他','RESPONSE','客户反馈信息','其他','91000');
--接触规则
INSERT INTO MMAPDM.MID_ITEM VALUES('ACPT_ATM','接受ATM营销','ACPT_TYPE','接触规则','ACPT_ATM','接受ATM营销','ACPT_TYPE','接触规则','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('ACPT_DM','接受邮件营销','ACPT_TYPE','接触规则','ACPT_DM','接受邮件营销','ACPT_TYPE','接触规则','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('ACPT_EDM','接受电子邮件营销','ACPT_TYPE','接触规则','ACPT_EDM','接受电子邮件营销','ACPT_TYPE','接触规则','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('ACPT_MB','接受手机银行营销','ACPT_TYPE','接触规则','ACPT_MB','接受手机银行营销','ACPT_TYPE','接触规则','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('ACPT_NB','接受网络银行营销','ACPT_TYPE','接触规则','ACPT_NB','接受网络银行营销','ACPT_TYPE','接触规则','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('ACPT_PERSON','接受人员直接营销','ACPT_TYPE','接触规则','ACPT_PERSON','接受人员直接营销','ACPT_TYPE','接触规则','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('ACPT_SMS','接受短信营销','ACPT_TYPE','接触规则','ACPT_SMS','接受短信营销','ACPT_TYPE','接触规则','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('ACPT_WEBATM','接受WEBATM营销','ACPT_TYPE','接触规则','ACPT_WEBATM','接受WEBATM营销','ACPT_TYPE','接触规则','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('ACPT_WECHAT','接受微信营销','ACPT_TYPE','接触规则','ACPT_WECHAT','接受微信营销','ACPT_TYPE','接触规则','','');
--合约类型
INSERT INTO MMAPDM.MID_ITEM VALUES('GAS','代缴燃气费户','AGMT_TYPE','合约类型','GAS','代缴燃气费户','AGMT_TYPE','合约类型','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('WATER','代缴水费','AGMT_TYPE','合约类型','WATER','代缴水费','AGMT_TYPE','合约类型','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('POWER','代缴电费','AGMT_TYPE','合约类型','POWER','代缴电费','AGMT_TYPE','合约类型','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('TELE_COMM','代缴电信费','AGMT_TYPE','合约类型','TELE_COMM','代缴电信费','AGMT_TYPE','合约类型','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('ALIPAY','支付宝户','AGMT_TYPE','合约类型','ALIPAY','支付宝户','AGMT_TYPE','合约类型','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('BP','青银直通车','AGMT_TYPE','合约类型','BP','青银直通车','AGMT_TYPE','合约类型','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('PAYROLL','代发工资','AGMT_TYPE','合约类型','PAYROLL','代发工资','AGMT_TYPE','合约类型','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('ENTRUST','委托代扣','AGMT_TYPE','合约类型','ENTRUST','委托代扣','AGMT_TYPE','合约类型','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('MB','手机银行','AGMT_TYPE','合约类型','MB','手机银行','AGMT_TYPE','合约类型','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('SMS','开通短信','AGMT_TYPE','合约类型','SMS','开通短信','AGMT_TYPE','合约类型','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('TELE','电话银行','AGMT_TYPE','合约类型','TELE','电话银行','AGMT_TYPE','合约类型','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('WECHAT','微信银行','AGMT_TYPE','合约类型','WECHAT','微信银行','AGMT_TYPE','合约类型','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('NB','网络银行','AGMT_TYPE','合约类型','NB','网络银行','AGMT_TYPE','合约类型','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('NB_ENT','网络银行企业版','AGMT_TYPE','合约类型','NB_ENT','网络银行企业版','AGMT_TYPE','合约类型','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('NB_LIFE','网络银行生活版','AGMT_TYPE','合约类型','NB_LIFE','网络银行生活版','AGMT_TYPE','合约类型','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('NB_WM','网络银行财富版','AGMT_TYPE','合约类型','NB_WM','网络银行财富版','AGMT_TYPE','合约类型','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('IC_CARD','芯片金融卡','AGMT_TYPE','合约类型','IC_CARD','芯片金融卡','AGMT_TYPE','合约类型','','');
--产品大类
INSERT INTO MMAPDM.MID_ITEM VALUES('CD','活期存款','PROD_TYPE','产品大类','CD','活期存款','PROD_TYPE','产品大类','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('CD_FC','外币活期存款','PROD_TYPE','产品大类','CD_FC','外币活期存款','PROD_TYPE','产品大类','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('TD','定期存款户','PROD_TYPE','产品大类','TD','定期存款户','PROD_TYPE','产品大类','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('TD_FC','外币定期存款户','PROD_TYPE','产品大类','TD_FC','外币定期存款户','PROD_TYPE','产品大类','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('FST','快易贷','PROD_TYPE','产品大类','FST','快易贷','PROD_TYPE','产品大类','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('FST_FC','外币快易贷','PROD_TYPE','产品大类','FST_FC','外币快易贷','PROD_TYPE','产品大类','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('HLN','房易贷','PROD_TYPE','产品大类','HLN','房易贷','PROD_TYPE','产品大类','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('HLN_FC','外币房易贷','PROD_TYPE','产品大类','HLN_FC','外币房易贷','PROD_TYPE','产品大类','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('CLN','车易贷','PROD_TYPE','产品大类','CLN','车易贷','PROD_TYPE','产品大类','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('CLN_FC','外币车易贷','PROD_TYPE','产品大类','CLN_FC','外币车易贷','PROD_TYPE','产品大类','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('PLN','信易贷','PROD_TYPE','产品大类','PLN','信易贷','PROD_TYPE','产品大类','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('PLN_FC','外币信易贷','PROD_TYPE','产品大类','PLN_FC','外币信易贷','PROD_TYPE','产品大类','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('FARM','农易贷','PROD_TYPE','产品大类','FARM','农易贷','PROD_TYPE','产品大类','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('FARM_FC','外币农易贷','PROD_TYPE','产品大类','FARM_FC','外币农易贷','PROD_TYPE','产品大类','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('NDEBT','国债','PROD_TYPE','产品大类','NDEBT','国债','PROD_TYPE','产品大类','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('NDEBT_FC','外币国债','PROD_TYPE','产品大类','NDEBT_FC','外币国债','PROD_TYPE','产品大类','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('FOUND','基金','PROD_TYPE','产品大类','FOUND','基金','PROD_TYPE','产品大类','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('FOUND_FC','外币基金','PROD_TYPE','产品大类','FOUND_FC','外币基金','PROD_TYPE','产品大类','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('FIN','理财','PROD_TYPE','产品大类','FIN','理财','PROD_TYPE','产品大类','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('FIN_FC','外币理财','PROD_TYPE','产品大类','FIN_FC','外币理财','PROD_TYPE','产品大类','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('INSURE','保险','PROD_TYPE','产品大类','INSURE','保险','PROD_TYPE','产品大类','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('INSURE_FC','外币保险','PROD_TYPE','产品大类','INSURE_FC','外币保险','PROD_TYPE','产品大类','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('NOBLE','贵金属','PROD_TYPE','产品大类','NOBLE','贵金属','PROD_TYPE','产品大类','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('NOBLE_FC','外币贵金属','PROD_TYPE','产品大类','NOBLE_FC','外币贵金属','PROD_TYPE','产品大类','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('CREDIT','信用卡','PROD_TYPE','产品大类','CREDIT','信用卡','PROD_TYPE','产品大类','','');


COMMIT;
*/

END P_MID_ITEM;
