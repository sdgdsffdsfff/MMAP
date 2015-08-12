truncate table mmapst.ST_ITEM;
INSERT INTO MMAPST.ST_ITEM

--客户风险等级_代码
select distinct TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,TRIM(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN '未知' ELSE TRIM(a.item_nm) END
       ,'RISK_LVL'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='Risk_Lvl'
UNION ALL
--客户级别
select distinct TRIM(a.item_id)
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
select distinct TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,TRIM(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN '未知' ELSE TRIM(a.item_nm) END
       ,'RACE'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='RaceTyp_ID'
UNION ALL
--国籍
select distinct TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,trim(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN '未知' ELSE TRIM(a.item_nm) END
       ,'COUNTRY'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='Citizen_ID'
UNION ALL
--职称
select distinct TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,trim(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN '未知' ELSE TRIM(a.item_nm) END
       ,'POSITION_TITLE'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='ProfTitl_ID'
UNION ALL
--职业
select distinct TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,trim(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN '未知' ELSE TRIM(a.item_nm) END
       ,'OCCP'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='Occp_ID'
UNION ALL
--职位
select distinct TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,trim(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN '未知' ELSE TRIM(a.item_nm) END
       ,'POSITION'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='JobDes_ID'
UNION ALL
--婚姻状态
select distinct TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,TRIM(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN '未知' ELSE TRIM(a.item_nm) END
       ,'MARITALSTS'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='MaritalSts_ID'
UNION ALL
--教育程度
select distinct TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,trim(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN '未知' ELSE TRIM(a.item_nm) END
       ,'EDU'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='Edu_ID'
UNION ALL
--性别
select distinct TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,TRIM(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN '未知' ELSE TRIM(a.item_nm) END
       ,'GENDER'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='Gender_ID'

--总账代码
UNION ALL
select distinct TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,trim(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN '未知' ELSE TRIM(a.item_nm) END
       ,'GLGRP_ID'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  MMAPST.Dmmkt_Pcodemast A
where item_typ='GLGrp_ID'
and bak3='PDAsset'
;

--借记卡级别
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
commit;
select * from   mmapst.ST_ITEM;
