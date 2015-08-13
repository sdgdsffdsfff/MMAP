truncate table mmapst.ST_ITEM;
INSERT INTO MMAPST.ST_ITEM

--客户风险等级_代码
select distinct	TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,TRIM(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN '未知'	ELSE TRIM(a.item_nm) END
       ,'RISK_LVL'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='Risk_Lvl'
UNION ALL
--客户级别
select distinct	TRIM(a.item_id)
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
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='CIF_Lvl'
UNION ALL
--民族
select distinct	TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,TRIM(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN '未知'	ELSE TRIM(a.item_nm) END
       ,'RACE'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='RaceTyp_ID'
UNION ALL
--国籍
select distinct	TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,trim(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN '未知'	ELSE TRIM(a.item_nm) END
       ,'COUNTRY'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='Citizen_ID'
UNION ALL
--职称
select distinct	TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,trim(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN '未知'	ELSE TRIM(a.item_nm) END
       ,'POSITION_TITLE'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='ProfTitl_ID'
UNION ALL
--职业
select distinct	TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,trim(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN '未知'	ELSE TRIM(a.item_nm) END
       ,'OCCP'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='Occp_ID'
UNION ALL
--职位
select distinct	TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,trim(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN '未知'	ELSE TRIM(a.item_nm) END
       ,'POSITION'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='JobDes_ID'
UNION ALL
--婚姻状态
select distinct	TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,TRIM(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN '未知'	ELSE TRIM(a.item_nm) END
       ,'MARITALSTS'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='MaritalSts_ID'
UNION ALL
--教育程度
select distinct	TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,trim(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN '未知'	ELSE TRIM(a.item_nm) END
       ,'EDU'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='Edu_ID'
UNION ALL
--性别
select distinct	TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,TRIM(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN '未知'	ELSE TRIM(a.item_nm) END
       ,'GENDER'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='Gender_ID'

--总账代码
UNION ALL
select distinct	TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,trim(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN '未知'	ELSE TRIM(a.item_nm) END
       ,'GLGRP_ID'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  MMAPST.Dmmkt_Pcodemast A
where item_typ='GLGrp_ID'
and bak3='PDAsset'
;

--借记卡级别
INSERT INTO MMAPST.ST_ITEM VALUES('-','','CrdTyp_ID','卡类型','WZ','未知','CARD_LVL','借记卡级别','','0');
INSERT INTO MMAPST.ST_ITEM VALUES('EBON','理财卡','CrdTyp_ID','卡类型','PK','普卡','CARD_LVL','借记卡级别','','10');
INSERT INTO MMAPST.ST_ITEM VALUES('LTMC','海融联通卡','CrdTyp_ID','卡类型','PK','普卡','CARD_LVL','借记卡级别','','10');
INSERT INTO MMAPST.ST_ITEM VALUES('NORI','新普通卡','CrdTyp_ID','卡类型','PK','普卡','CARD_LVL','借记卡级别','','10');
INSERT INTO MMAPST.ST_ITEM VALUES('NORM','普通卡','CrdTyp_ID','卡类型','PK','普卡','CARD_LVL','借记卡级别','','10');
INSERT INTO MMAPST.ST_ITEM VALUES('LQNC','利群普通卡','CrdTyp_ID','卡类型','PK','普卡','CARD_LVL','借记卡级别','','10');
INSERT INTO MMAPST.ST_ITEM VALUES('QQCC','海融青春卡','CrdTyp_ID','卡类型','PK','普卡','CARD_LVL','借记卡级别','','10');
INSERT INTO MMAPST.ST_ITEM VALUES('QQDC','海融青大卡','CrdTyp_ID','卡类型','PK','普卡','CARD_LVL','借记卡级别','','10');
INSERT INTO MMAPST.ST_ITEM VALUES('SIXE','6+E卡','CrdTyp_ID','卡类型','PK','普卡','CARD_LVL','借记卡级别','','10');
INSERT INTO MMAPST.ST_ITEM VALUES('SOCS','社保金融卡','CrdTyp_ID','卡类型','PK','普卡','CARD_LVL','借记卡级别','','10');
INSERT INTO MMAPST.ST_ITEM VALUES('SSYC','世园卡','CrdTyp_ID','卡类型','PK','普卡','CARD_LVL','借记卡级别','','10');
INSERT INTO MMAPST.ST_ITEM VALUES('TSMC','TSM电子现金卡','CrdTyp_ID','卡类型','PK','普卡','CARD_LVL','借记卡级别','','10');
INSERT INTO MMAPST.ST_ITEM VALUES('VCRD','联名卡','CrdTyp_ID','卡类型','PK','普卡','CARD_LVL','借记卡级别','','10');
INSERT INTO MMAPST.ST_ITEM VALUES('WCRD','微尘卡','CrdTyp_ID','卡类型','PK','普卡','CARD_LVL','借记卡级别','','10');
INSERT INTO MMAPST.ST_ITEM VALUES('YKTC','海融普通卡','CrdTyp_ID','卡类型','PK','普卡','CARD_LVL','借记卡级别','','10');
INSERT INTO MMAPST.ST_ITEM VALUES('NOGC','金卡','CrdTyp_ID','卡类型','JK','金卡','CARD_LVL','借记卡级别','','20');
INSERT INTO MMAPST.ST_ITEM VALUES('YKTG','海融金卡','CrdTyp_ID','卡类型','JK','金卡','CARD_LVL','借记卡级别','','20');
INSERT INTO MMAPST.ST_ITEM VALUES('LQGC','利群金卡','CrdTyp_ID','卡类型','JK','金卡','CARD_LVL','借记卡级别','','20');
INSERT INTO MMAPST.ST_ITEM VALUES('YKTP','海融白金卡','CrdTyp_ID','卡类型','BJK','白金卡','CARD_LVL','借记卡级别','','30');
INSERT INTO MMAPST.ST_ITEM VALUES('LQPC','利群白金卡','CrdTyp_ID','卡类型','BJK','白金卡','CARD_LVL','借记卡级别','','30');
INSERT INTO MMAPST.ST_ITEM VALUES('NOPC','白金卡','CrdTyp_ID','卡类型','BJK','白金卡','CARD_LVL','借记卡级别','','30');
INSERT INTO MMAPST.ST_ITEM VALUES('DCRD','海融钻石卡','CrdTyp_ID','卡类型','ZSK','钻石卡','CARD_LVL','借记卡级别','','40');
INSERT INTO MMAPST.ST_ITEM VALUES('CCRD','海融财富卡','CrdTyp_ID','卡类型','CFK','财富卡','CARD_LVL','借记卡级别','','50');
INSERT INTO MMAPST.ST_ITEM VALUES('PVBC','海融私人银行卡','CrdTyp_ID','卡类型','SRYH','私人银行','CARD_LVL','借记卡级别','','60');

--客户反馈代码
INSERT INTO MMAPST.ST_ITEM VALUES('10001','完成营销/任务(关怀,满意度调查..),不需转介','RESPONSE','客户反馈代码','10001','意愿明确，愿意购买','RESPONSE','客户反馈信息','客户有意愿','10000');
INSERT INTO MMAPST.ST_ITEM VALUES('10002','有意愿购买，愿意再深入、详细了解','RESPONSE','客户反馈代码','10002','有意愿购买，愿意再深入、详细了解','RESPONSE','客户反馈信息','客户有意愿','10000');
INSERT INTO MMAPST.ST_ITEM VALUES('10003','有购买意愿，但目前资金不到位','RESPONSE','客户反馈代码','10003','有购买意愿，但目前资金不到位','RESPONSE','客户反馈信息','客户有意愿','10000');
INSERT INTO MMAPST.ST_ITEM VALUES('10004','同渠道页面跳转或点击超链接','RESPONSE','客户反馈代码','10004','有意愿，电子渠道页面跳转','RESPONSE','客户反馈信息','客户有意愿','10000');
INSERT INTO MMAPST.ST_ITEM VALUES('10005','其他','RESPONSE','客户反馈代码','10005','其他','RESPONSE','客户反馈信息','客户有意愿','10000');
INSERT INTO MMAPST.ST_ITEM VALUES('20001','产品不符合客户需求--产品起点过高','RESPONSE','客户反馈代码','20001','产品不符合(客户)需求--产品起点过高','RESPONSE','客户反馈信息','客户无意愿','20000');
INSERT INTO MMAPST.ST_ITEM VALUES('20002','产品不符合客户需求--客户偏好定期类稳健产品','RESPONSE','客户反馈代码','20002','产品不符合客户需求--客户偏好定期类稳健产品','RESPONSE','客户反馈信息','客户无意愿','20000');
INSERT INTO MMAPST.ST_ITEM VALUES('20003','产品不符合客户需求--客户偏好股市、基金等风险投资','RESPONSE','客户反馈代码','20003','产品不符合客户需求--客户偏好股市、基金等风险投资','RESPONSE','客户反馈信息','客户无意愿','20000');
INSERT INTO MMAPST.ST_ITEM VALUES('20004','产品不符合客户需求--其他','RESPONSE','客户反馈代码','20004','产品不符合客户需求--其他','RESPONSE','客户反馈信息','客户无意愿','20000');
INSERT INTO MMAPST.ST_ITEM VALUES('20005','产品不符合(客户)需求','RESPONSE','客户反馈代码','20005','无意愿，产品不能满足需求','RESPONSE','客户反馈信息','客户无意愿','20000');
INSERT INTO MMAPST.ST_ITEM VALUES('20006','需深入营销','RESPONSE','客户反馈代码','20006','其他','RESPONSE','客户反馈信息','客户无意愿','20000');
INSERT INTO MMAPST.ST_ITEM VALUES('20007','客户表示没兴趣','RESPONSE','客户反馈代码','20007','无意愿，对产品不感兴趣','RESPONSE','客户反馈信息','客户无意愿','20000');
INSERT INTO MMAPST.ST_ITEM VALUES('20999','其他(请说明)','RESPONSE','客户反馈代码','20999','无意愿，拒绝告知原因','RESPONSE','客户反馈信息','客户无意愿','20000');
INSERT INTO MMAPST.ST_ITEM VALUES('30001','拒绝营销','RESPONSE','客户反馈代码','30001','拒绝营销，不希望再被骚扰','RESPONSE','客户反馈信息','客户拒绝接触营销','30000');
INSERT INTO MMAPST.ST_ITEM VALUES('60001','电话错误/空号','RESPONSE','客户反馈代码','60001','无效号码或空号','RESPONSE','客户反馈信息','无效名单','60000');
INSERT INTO MMAPST.ST_ITEM VALUES('60002','更换电话/无此人','RESPONSE','客户反馈代码','60002','错误号码，非客户本人','RESPONSE','客户反馈信息','无效名单','60000');
INSERT INTO MMAPST.ST_ITEM VALUES('60003','无人接听(达一定次数以上)','RESPONSE','客户反馈代码','60003','多次无人接听','RESPONSE','客户反馈信息','无效名单','60000');
INSERT INTO MMAPST.ST_ITEM VALUES('60004','E-mail错误(寄送失败）','RESPONSE','客户反馈代码','60004','错误电邮地址','RESPONSE','客户反馈信息','无效名单','60000');
INSERT INTO MMAPST.ST_ITEM VALUES('60999','其他','RESPONSE','客户反馈代码','60999','其他','RESPONSE','客户反馈信息','无效名单','60000');
INSERT INTO MMAPST.ST_ITEM VALUES('99999','无任何反应','RESPONSE','客户反馈代码','99999','客户无任何反馈','RESPONSE','客户反馈信息','无效名单','60000');
INSERT INTO MMAPST.ST_ITEM VALUES('80001','考虑中，待追踪','RESPONSE','客户反馈代码','80001','考虑中，待追踪','RESPONSE','客户反馈信息','待追踪','80000');
INSERT INTO MMAPST.ST_ITEM VALUES('80002','不方便接听','RESPONSE','客户反馈代码','80002','不方便接听','RESPONSE','客户反馈信息','待追踪','80000');
INSERT INTO MMAPST.ST_ITEM VALUES('80003','不在(非本人接听)','RESPONSE','客户反馈代码','80003','不在(非本人接听)','RESPONSE','客户反馈信息','待追踪','80000');
INSERT INTO MMAPST.ST_ITEM VALUES('80004','非决策者,需联系他人','RESPONSE','客户反馈代码','80004','非决策者,需联系他人','RESPONSE','客户反馈信息','待追踪','80000');
INSERT INTO MMAPST.ST_ITEM VALUES('80999','其他','RESPONSE','客户反馈代码','80999','其他','RESPONSE','客户反馈信息','待追踪','80000');
INSERT INTO MMAPST.ST_ITEM VALUES('90000','未处理','RESPONSE','客户反馈代码','90000','未处理','RESPONSE','客户反馈信息','90 未处理','90000');
INSERT INTO MMAPST.ST_ITEM VALUES('91000','逾期未处理','RESPONSE','客户反馈代码','91000','逾期未处理','RESPONSE','客户反馈信息','91 逾期未处理','91000');

commit;
select * from	mmapst.ST_ITEM;
