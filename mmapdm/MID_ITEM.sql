CREATE OR REPLACE PROCEDURE P_MID_ITEM
AS
/*
truncate table MMAPDM.MID_ITEM;
INSERT INTO MMAPDM.MID_ITEM

--�ͻ����յȼ�_����
select distinct	TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,TRIM(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN 'δ֪'	ELSE TRIM(a.item_nm) END
       ,'RISK_LVL'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='Risk_Lvl'
UNION ALL
--�ͻ�����
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
	     WHEN '00' THEN '�տ�'
	     WHEN '01' THEN '��'
	     WHEN '50' THEN '��'
	     WHEN '55' THEN '�׽�'
	     WHEN '60' THEN '��ʯ��'
	     WHEN '70' THEN '�Ƹ���'
	     WHEN '80' THEN '˽������'
	 ELSE 'δ֪'
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
--����
select distinct	TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,TRIM(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN 'δ֪'	ELSE TRIM(a.item_nm) END
       ,'RACE'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='RaceTyp_ID'
UNION ALL
--����
select distinct	TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,trim(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN 'δ֪'	ELSE TRIM(a.item_nm) END
       ,'COUNTRY'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='Citizen_ID'
UNION ALL
--ְ��
select distinct	TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,trim(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN 'δ֪'	ELSE TRIM(a.item_nm) END
       ,'POSITION_TITLE'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='ProfTitl_ID'
UNION ALL
--ְҵ
select distinct	TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,trim(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN 'δ֪'	ELSE TRIM(a.item_nm) END
       ,'OCCP'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='Occp_ID'
UNION ALL
--ְλ
select distinct	TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,trim(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN 'δ֪'	ELSE TRIM(a.item_nm) END
       ,'POSITION'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='JobDes_ID'
UNION ALL
--����״̬
select distinct	TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,TRIM(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN 'δ֪'	ELSE TRIM(a.item_nm) END
       ,'MARITALSTS'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='MaritalSts_ID'
UNION ALL
--�����̶�
select distinct	TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,trim(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN 'δ֪'	ELSE TRIM(a.item_nm) END
       ,'EDU'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='Edu_ID'
UNION ALL
--�Ա�
select distinct	TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,TRIM(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN 'δ֪'	ELSE TRIM(a.item_nm) END
       ,'GENDER'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='Gender_ID'

--���˴���
UNION ALL
select distinct	TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,trim(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN 'δ֪'	ELSE TRIM(a.item_nm) END
       ,'GLGRP_ID'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  MMAPST.Dmmkt_Pcodemast A
where item_typ='GLGrp_ID'
and bak3='PDAsset'
--��ǿ����
UNION ALL
select distinct	TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,trim(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN 'δ֪'	ELSE TRIM(a.item_nm) END
       ,'CRDTYP'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  MMAPST.Dmmkt_Pcodemast A
where item_typ='CrdTyp_ID'
;
--��ǿ�����
INSERT INTO MMAPDM.MID_ITEM VALUES('-','','CrdTyp_ID','������','WZ','δ֪','CARD_LVL','��ǿ�����','','0');
INSERT INTO MMAPDM.MID_ITEM VALUES('EBON','��ƿ�','CrdTyp_ID','������','PK','�տ�','CARD_LVL','��ǿ�����','','10');
INSERT INTO MMAPDM.MID_ITEM VALUES('LTMC','������ͨ��','CrdTyp_ID','������','PK','�տ�','CARD_LVL','��ǿ�����','','10');
INSERT INTO MMAPDM.MID_ITEM VALUES('NORI','����ͨ��','CrdTyp_ID','������','PK','�տ�','CARD_LVL','��ǿ�����','','10');
INSERT INTO MMAPDM.MID_ITEM VALUES('NORM','��ͨ��','CrdTyp_ID','������','PK','�տ�','CARD_LVL','��ǿ�����','','10');
INSERT INTO MMAPDM.MID_ITEM VALUES('LQNC','��Ⱥ��ͨ��','CrdTyp_ID','������','PK','�տ�','CARD_LVL','��ǿ�����','','10');
INSERT INTO MMAPDM.MID_ITEM VALUES('QQCC','�����ഺ��','CrdTyp_ID','������','PK','�տ�','CARD_LVL','��ǿ�����','','10');
INSERT INTO MMAPDM.MID_ITEM VALUES('QQDC','�������','CrdTyp_ID','������','PK','�տ�','CARD_LVL','��ǿ�����','','10');
INSERT INTO MMAPDM.MID_ITEM VALUES('SIXE','6+E��','CrdTyp_ID','������','PK','�տ�','CARD_LVL','��ǿ�����','','10');
INSERT INTO MMAPDM.MID_ITEM VALUES('SOCS','�籣���ڿ�','CrdTyp_ID','������','PK','�տ�','CARD_LVL','��ǿ�����','','10');
INSERT INTO MMAPDM.MID_ITEM VALUES('SSYC','��԰��','CrdTyp_ID','������','PK','�տ�','CARD_LVL','��ǿ�����','','10');
INSERT INTO MMAPDM.MID_ITEM VALUES('TSMC','TSM�����ֽ�','CrdTyp_ID','������','PK','�տ�','CARD_LVL','��ǿ�����','','10');
INSERT INTO MMAPDM.MID_ITEM VALUES('VCRD','������','CrdTyp_ID','������','PK','�տ�','CARD_LVL','��ǿ�����','','10');
INSERT INTO MMAPDM.MID_ITEM VALUES('WCRD','΢����','CrdTyp_ID','������','PK','�տ�','CARD_LVL','��ǿ�����','','10');
INSERT INTO MMAPDM.MID_ITEM VALUES('YKTC','������ͨ��','CrdTyp_ID','������','PK','�տ�','CARD_LVL','��ǿ�����','','10');
INSERT INTO MMAPDM.MID_ITEM VALUES('NOGC','��','CrdTyp_ID','������','JK','��','CARD_LVL','��ǿ�����','','20');
INSERT INTO MMAPDM.MID_ITEM VALUES('YKTG','���ڽ�','CrdTyp_ID','������','JK','��','CARD_LVL','��ǿ�����','','20');
INSERT INTO MMAPDM.MID_ITEM VALUES('LQGC','��Ⱥ��','CrdTyp_ID','������','JK','��','CARD_LVL','��ǿ�����','','20');
INSERT INTO MMAPDM.MID_ITEM VALUES('YKTP','���ڰ׽�','CrdTyp_ID','������','BJK','�׽�','CARD_LVL','��ǿ�����','','30');
INSERT INTO MMAPDM.MID_ITEM VALUES('LQPC','��Ⱥ�׽�','CrdTyp_ID','������','BJK','�׽�','CARD_LVL','��ǿ�����','','30');
INSERT INTO MMAPDM.MID_ITEM VALUES('NOPC','�׽�','CrdTyp_ID','������','BJK','�׽�','CARD_LVL','��ǿ�����','','30');
INSERT INTO MMAPDM.MID_ITEM VALUES('DCRD','������ʯ��','CrdTyp_ID','������','ZSK','��ʯ��','CARD_LVL','��ǿ�����','','40');
INSERT INTO MMAPDM.MID_ITEM VALUES('CCRD','���ڲƸ���','CrdTyp_ID','������','CFK','�Ƹ���','CARD_LVL','��ǿ�����','','50');
INSERT INTO MMAPDM.MID_ITEM VALUES('PVBC','����˽�����п�','CrdTyp_ID','������','SRYH','˽������','CARD_LVL','��ǿ�����','','60');


--�ͻ���������
INSERT INTO MMAPDM.MID_ITEM VALUES('10001','���Ӫ��/����(�ػ�,����ȵ���..),����ת��','RESPONSE','�ͻ���������','10001','Ը�⹺��','RESPONSE','�ͻ�������Ϣ','��Ը��ȷ��Ը�⹺��','10000');
INSERT INTO MMAPDM.MID_ITEM VALUES('10002','','RESPONSE','�ͻ���������','10002','','RESPONSE','�ͻ�������Ϣ','���Ӫ��,��ת�飨������','10000');
INSERT INTO MMAPDM.MID_ITEM VALUES('10003','','RESPONSE','�ͻ���������','10003','','RESPONSE','�ͻ�������Ϣ','���Ӫ��,��ת�飨ʵʱ��','10000');
INSERT INTO MMAPDM.MID_ITEM VALUES('10004','ͬ����ҳ����ת����������','RESPONSE','�ͻ���������','10004','�ͻ�����Ը��ֱ��ͨ��Ӫ��ҳ����ת������ҳ�档','RESPONSE','�ͻ�������Ϣ','����Ը����������ҳ����ת','10000');
INSERT INTO MMAPDM.MID_ITEM VALUES('10005','����Ը����Ը�������롢��ϸ�˽�','RESPONSE','�ͻ���������','10005','����Ը����Ը�������롢��ϸ�˽�','RESPONSE','�ͻ�������Ϣ','����Ը����Ը�������롢��ϸ�˽�','10000');
INSERT INTO MMAPDM.MID_ITEM VALUES('10006','�й�����Ը����Ŀǰ�ʽ𲻵�λ','RESPONSE','�ͻ���������','20001','�й�����Ը����Ŀǰ�ʽ𲻵�λ','RESPONSE','�ͻ�������Ϣ','�й�����Ը����Ŀǰ�ʽ𲻵�λ','20000');
INSERT INTO MMAPDM.MID_ITEM VALUES('10999','����','RESPONSE','�ͻ���������','20002','����','RESPONSE','�ͻ�������Ϣ','����Ը-����','20000');
INSERT INTO MMAPDM.MID_ITEM VALUES('20001','��Ʒ�����Ͽͻ�����--��Ʒ������','RESPONSE','�ͻ���������','20003','��Ʒ�����Ͽͻ�����--��Ʒ������','RESPONSE','�ͻ�������Ϣ','��Ʒ������(�ͻ�)����--��Ʒ������','20000');
INSERT INTO MMAPDM.MID_ITEM VALUES('20002','��Ʒ�����Ͽͻ�����--�ͻ�ƫ�ö������Ƚ���Ʒ','RESPONSE','�ͻ���������','20004','��Ʒ�����Ͽͻ�����--�ͻ�ƫ�ö������Ƚ���Ʒ','RESPONSE','�ͻ�������Ϣ','��Ʒ�����Ͽͻ�����--�ͻ�ƫ�ö������Ƚ���Ʒ','20000');
INSERT INTO MMAPDM.MID_ITEM VALUES('20003','��Ʒ�����Ͽͻ�����--�ͻ�ƫ�ù��С�����ȷ���Ͷ��','RESPONSE','�ͻ���������','20005','��Ʒ�����Ͽͻ�����--�ͻ�ƫ�ù��С�����ȷ���Ͷ��','RESPONSE','�ͻ�������Ϣ','��Ʒ�����Ͽͻ�����--�ͻ�ƫ�ù��С�����ȷ���Ͷ��','20000');
INSERT INTO MMAPDM.MID_ITEM VALUES('20004','��Ʒ�����Ͽͻ�����--����','RESPONSE','�ͻ���������','20006','��Ʒ�����Ͽͻ�����--����','RESPONSE','�ͻ�������Ϣ','��Ʒ�����Ͽͻ�����--����','20000');
INSERT INTO MMAPDM.MID_ITEM VALUES('20005','��Ʒ������(�ͻ�)����','RESPONSE','�ͻ���������','20007','�ò�Ʒ��������ͻ������ĳһ������','RESPONSE','�ͻ�������Ϣ','����Ը����Ʒ������������','20000');
INSERT INTO MMAPDM.MID_ITEM VALUES('20006','������Ӫ��','RESPONSE','�ͻ���������','20999','������Ӫ��','RESPONSE','�ͻ�������Ϣ','������Ӫ��','20000');
INSERT INTO MMAPDM.MID_ITEM VALUES('20007','�ͻ���ʾû��Ȥ','RESPONSE','�ͻ���������','30001','�ͻ���������Ҫ�����Ʒ','RESPONSE','�ͻ�������Ϣ','����Ը���Բ�Ʒ������Ȥ','30000');
INSERT INTO MMAPDM.MID_ITEM VALUES('20999','����(��˵��)','RESPONSE','�ͻ���������','60001','�ͻ�δӦ�𣬾ܾ���֪ԭ��','RESPONSE','�ͻ�������Ϣ','����Ը���ܾ���֪ԭ��','60000');
INSERT INTO MMAPDM.MID_ITEM VALUES('30001','�ܾ�Ӫ��','RESPONSE','�ͻ���������','60002','����Ҫ���Ҵ�绰��','RESPONSE','�ͻ�������Ϣ','�ܾ�Ӫ������ϣ���ٱ�ɧ��','60000');
INSERT INTO MMAPDM.MID_ITEM VALUES('60001','�绰����/�պ�','RESPONSE','�ͻ���������','60003','�����ĺ��������õ绰�ǿպ�','RESPONSE','�ͻ�������Ϣ','��Ч�����պ�','60000');
INSERT INTO MMAPDM.MID_ITEM VALUES('60002','�����绰/�޴���','RESPONSE','�ͻ���������','60004','�ӵ绰�ǿͻ����ˣ�������ͻ�������ʶ','RESPONSE','�ͻ�������Ϣ','������룬�ǿͻ�����','60000');
INSERT INTO MMAPDM.MID_ITEM VALUES('60003','���˽���(��һ����������)','RESPONSE','�ͻ���������','60999','�绰��ͨ�������˽���','RESPONSE','�ͻ�������Ϣ','������˽���','60000');
INSERT INTO MMAPDM.MID_ITEM VALUES('60005','E-mail����(����ʧ�ܣ�','RESPONSE','�ͻ���������','99999','E-mail�����ţ��޷��ʹ�Ŀ���ַ','RESPONSE','�ͻ�������Ϣ','������ʵ�ַ','60000');
INSERT INTO MMAPDM.MID_ITEM VALUES('60999','����','RESPONSE','�ͻ���������','80001','����','RESPONSE','�ͻ�������Ϣ','����','80000');
INSERT INTO MMAPDM.MID_ITEM VALUES('99999','���κη�Ӧ','RESPONSE','�ͻ���������','80002','�ǵ绰������������š��������ͻ�����Ӫ����Ϣ��û���κη�����','RESPONSE','�ͻ�������Ϣ','�ͻ����κη���','80000');
INSERT INTO MMAPDM.MID_ITEM VALUES('80001','�����У���׷��','RESPONSE','�ͻ���������','80003','�ͻ��Բ�Ʒ��ʾ���ģ�û����ȷ��Ը����Ҫ�ٴλز��绰������ͨ','RESPONSE','�ͻ�������Ϣ','�����У���׷��','80000');
INSERT INTO MMAPDM.MID_ITEM VALUES('80002','���������','RESPONSE','�ͻ���������','80004','�޹��ڲ�Ʒ�����۶Ի������ڻ����л���������������ͻ����������','RESPONSE','�ͻ�������Ϣ','���������','80000');
INSERT INTO MMAPDM.MID_ITEM VALUES('80003','����(�Ǳ��˽���)','RESPONSE','�ͻ���������','80999','�ͻ��绰���󣬵����Ǳ��˽�������Ҫ��ʱ�ٻز���ϵ���ˡ�','RESPONSE','�ͻ�������Ϣ','����(�Ǳ��˽���)','80000');
INSERT INTO MMAPDM.MID_ITEM VALUES('80004','�Ǿ�����,����ϵ����','RESPONSE','�ͻ���������','90000','�ͻ����˲��ܵ�������������Ҫ�����������������������','RESPONSE','�ͻ�������Ϣ','�Ǿ�����,����ϵ����','90000');
INSERT INTO MMAPDM.MID_ITEM VALUES('80999','����','RESPONSE','�ͻ���������','91000','����','RESPONSE','�ͻ�������Ϣ','����','91000');
--�Ӵ�����
INSERT INTO MMAPDM.MID_ITEM VALUES('ACPT_ATM','����ATMӪ��','ACPT_TYPE','�Ӵ�����','ACPT_ATM','����ATMӪ��','ACPT_TYPE','�Ӵ�����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('ACPT_DM','�����ʼ�Ӫ��','ACPT_TYPE','�Ӵ�����','ACPT_DM','�����ʼ�Ӫ��','ACPT_TYPE','�Ӵ�����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('ACPT_EDM','���ܵ����ʼ�Ӫ��','ACPT_TYPE','�Ӵ�����','ACPT_EDM','���ܵ����ʼ�Ӫ��','ACPT_TYPE','�Ӵ�����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('ACPT_MB','�����ֻ�����Ӫ��','ACPT_TYPE','�Ӵ�����','ACPT_MB','�����ֻ�����Ӫ��','ACPT_TYPE','�Ӵ�����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('ACPT_NB','������������Ӫ��','ACPT_TYPE','�Ӵ�����','ACPT_NB','������������Ӫ��','ACPT_TYPE','�Ӵ�����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('ACPT_PERSON','������Աֱ��Ӫ��','ACPT_TYPE','�Ӵ�����','ACPT_PERSON','������Աֱ��Ӫ��','ACPT_TYPE','�Ӵ�����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('ACPT_SMS','���ܶ���Ӫ��','ACPT_TYPE','�Ӵ�����','ACPT_SMS','���ܶ���Ӫ��','ACPT_TYPE','�Ӵ�����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('ACPT_WEBATM','����WEBATMӪ��','ACPT_TYPE','�Ӵ�����','ACPT_WEBATM','����WEBATMӪ��','ACPT_TYPE','�Ӵ�����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('ACPT_WECHAT','����΢��Ӫ��','ACPT_TYPE','�Ӵ�����','ACPT_WECHAT','����΢��Ӫ��','ACPT_TYPE','�Ӵ�����','','');
--��Լ����
INSERT INTO MMAPDM.MID_ITEM VALUES('GAS','����ȼ���ѻ�','AGMT_TYPE','��Լ����','GAS','����ȼ���ѻ�','AGMT_TYPE','��Լ����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('WATER','����ˮ��','AGMT_TYPE','��Լ����','WATER','����ˮ��','AGMT_TYPE','��Լ����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('POWER','���ɵ��','AGMT_TYPE','��Լ����','POWER','���ɵ��','AGMT_TYPE','��Լ����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('TELE_COMM','���ɵ��ŷ�','AGMT_TYPE','��Լ����','TELE_COMM','���ɵ��ŷ�','AGMT_TYPE','��Լ����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('ALIPAY','֧������','AGMT_TYPE','��Լ����','ALIPAY','֧������','AGMT_TYPE','��Լ����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('BP','����ֱͨ��','AGMT_TYPE','��Լ����','BP','����ֱͨ��','AGMT_TYPE','��Լ����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('PAYROLL','��������','AGMT_TYPE','��Լ����','PAYROLL','��������','AGMT_TYPE','��Լ����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('ENTRUST','ί�д���','AGMT_TYPE','��Լ����','ENTRUST','ί�д���','AGMT_TYPE','��Լ����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('MB','�ֻ�����','AGMT_TYPE','��Լ����','MB','�ֻ�����','AGMT_TYPE','��Լ����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('SMS','��ͨ����','AGMT_TYPE','��Լ����','SMS','��ͨ����','AGMT_TYPE','��Լ����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('TELE','�绰����','AGMT_TYPE','��Լ����','TELE','�绰����','AGMT_TYPE','��Լ����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('WECHAT','΢������','AGMT_TYPE','��Լ����','WECHAT','΢������','AGMT_TYPE','��Լ����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('NB','��������','AGMT_TYPE','��Լ����','NB','��������','AGMT_TYPE','��Լ����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('NB_ENT','����������ҵ��','AGMT_TYPE','��Լ����','NB_ENT','����������ҵ��','AGMT_TYPE','��Լ����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('NB_LIFE','�������������','AGMT_TYPE','��Լ����','NB_LIFE','�������������','AGMT_TYPE','��Լ����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('NB_WM','�������вƸ���','AGMT_TYPE','��Լ����','NB_WM','�������вƸ���','AGMT_TYPE','��Լ����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('IC_CARD','оƬ���ڿ�','AGMT_TYPE','��Լ����','IC_CARD','оƬ���ڿ�','AGMT_TYPE','��Լ����','','');
--��Ʒ����
INSERT INTO MMAPDM.MID_ITEM VALUES('CD','���ڴ��','PROD_TYPE','��Ʒ����','CD','���ڴ��','PROD_TYPE','��Ʒ����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('CD_FC','��һ��ڴ��','PROD_TYPE','��Ʒ����','CD_FC','��һ��ڴ��','PROD_TYPE','��Ʒ����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('TD','���ڴ�','PROD_TYPE','��Ʒ����','TD','���ڴ�','PROD_TYPE','��Ʒ����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('TD_FC','��Ҷ��ڴ�','PROD_TYPE','��Ʒ����','TD_FC','��Ҷ��ڴ�','PROD_TYPE','��Ʒ����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('FST','���״�','PROD_TYPE','��Ʒ����','FST','���״�','PROD_TYPE','��Ʒ����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('FST_FC','��ҿ��״�','PROD_TYPE','��Ʒ����','FST_FC','��ҿ��״�','PROD_TYPE','��Ʒ����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('HLN','���״�','PROD_TYPE','��Ʒ����','HLN','���״�','PROD_TYPE','��Ʒ����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('HLN_FC','��ҷ��״�','PROD_TYPE','��Ʒ����','HLN_FC','��ҷ��״�','PROD_TYPE','��Ʒ����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('CLN','���״�','PROD_TYPE','��Ʒ����','CLN','���״�','PROD_TYPE','��Ʒ����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('CLN_FC','��ҳ��״�','PROD_TYPE','��Ʒ����','CLN_FC','��ҳ��״�','PROD_TYPE','��Ʒ����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('PLN','���״�','PROD_TYPE','��Ʒ����','PLN','���״�','PROD_TYPE','��Ʒ����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('PLN_FC','������״�','PROD_TYPE','��Ʒ����','PLN_FC','������״�','PROD_TYPE','��Ʒ����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('FARM','ũ�״�','PROD_TYPE','��Ʒ����','FARM','ũ�״�','PROD_TYPE','��Ʒ����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('FARM_FC','���ũ�״�','PROD_TYPE','��Ʒ����','FARM_FC','���ũ�״�','PROD_TYPE','��Ʒ����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('NDEBT','��ծ','PROD_TYPE','��Ʒ����','NDEBT','��ծ','PROD_TYPE','��Ʒ����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('NDEBT_FC','��ҹ�ծ','PROD_TYPE','��Ʒ����','NDEBT_FC','��ҹ�ծ','PROD_TYPE','��Ʒ����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('FOUND','����','PROD_TYPE','��Ʒ����','FOUND','����','PROD_TYPE','��Ʒ����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('FOUND_FC','��һ���','PROD_TYPE','��Ʒ����','FOUND_FC','��һ���','PROD_TYPE','��Ʒ����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('FIN','���','PROD_TYPE','��Ʒ����','FIN','���','PROD_TYPE','��Ʒ����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('FIN_FC','������','PROD_TYPE','��Ʒ����','FIN_FC','������','PROD_TYPE','��Ʒ����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('INSURE','����','PROD_TYPE','��Ʒ����','INSURE','����','PROD_TYPE','��Ʒ����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('INSURE_FC','��ұ���','PROD_TYPE','��Ʒ����','INSURE_FC','��ұ���','PROD_TYPE','��Ʒ����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('NOBLE','�����','PROD_TYPE','��Ʒ����','NOBLE','�����','PROD_TYPE','��Ʒ����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('NOBLE_FC','��ҹ����','PROD_TYPE','��Ʒ����','NOBLE_FC','��ҹ����','PROD_TYPE','��Ʒ����','','');
INSERT INTO MMAPDM.MID_ITEM VALUES('CREDIT','���ÿ�','PROD_TYPE','��Ʒ����','CREDIT','���ÿ�','PROD_TYPE','��Ʒ����','','');


commit;
*/
END P_MID_ITEM;
