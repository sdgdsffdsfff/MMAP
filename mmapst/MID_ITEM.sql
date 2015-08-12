truncate table mmapst.ST_ITEM;
INSERT INTO MMAPST.ST_ITEM

--�ͻ����յȼ�_����
select distinct TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,TRIM(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN 'δ֪' ELSE TRIM(a.item_nm) END
       ,'RISK_LVL'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='Risk_Lvl'
UNION ALL
--�ͻ�����
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
select distinct TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,TRIM(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN 'δ֪' ELSE TRIM(a.item_nm) END
       ,'RACE'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='RaceTyp_ID'
UNION ALL
--����
select distinct TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,trim(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN 'δ֪' ELSE TRIM(a.item_nm) END
       ,'COUNTRY'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='Citizen_ID'
UNION ALL
--ְ��
select distinct TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,trim(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN 'δ֪' ELSE TRIM(a.item_nm) END
       ,'POSITION_TITLE'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='ProfTitl_ID'
UNION ALL
--ְҵ
select distinct TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,trim(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN 'δ֪' ELSE TRIM(a.item_nm) END
       ,'OCCP'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='Occp_ID'
UNION ALL
--ְλ
select distinct TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,trim(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN 'δ֪' ELSE TRIM(a.item_nm) END
       ,'POSITION'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='JobDes_ID'
UNION ALL
--����״̬
select distinct TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,TRIM(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN 'δ֪' ELSE TRIM(a.item_nm) END
       ,'MARITALSTS'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='MaritalSts_ID'
UNION ALL
--�����̶�
select distinct TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,trim(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN 'δ֪' ELSE TRIM(a.item_nm) END
       ,'EDU'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='Edu_ID'
UNION ALL
--�Ա�
select distinct TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,TRIM(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN 'δ֪' ELSE TRIM(a.item_nm) END
       ,'GENDER'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='Gender_ID'

--���˴���
UNION ALL
select distinct TRIM(a.item_id)
       ,TRIM(a.item_nm)
       ,TRIM(a.item_typ)
       ,TRIM(a.item_des)
       ,trim(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN 'δ֪' ELSE TRIM(a.item_nm) END
       ,'GLGRP_ID'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  MMAPST.Dmmkt_Pcodemast A
where item_typ='GLGrp_ID'
and bak3='PDAsset'
;

--��ǿ�����
INSERT INTO MMAPST.ST_ITEM VALUES('EBON','��ƿ�','CrdTyp_ID','������','PK','�տ�','CARD_LVL','��ǿ�����','','10');
INSERT INTO MMAPST.ST_ITEM VALUES('LTMC','������ͨ��','CrdTyp_ID','������','PK','�տ�','CARD_LVL','��ǿ�����','','10');
INSERT INTO MMAPST.ST_ITEM VALUES('NORI','����ͨ��','CrdTyp_ID','������','PK','�տ�','CARD_LVL','��ǿ�����','','10');
INSERT INTO MMAPST.ST_ITEM VALUES('NORM','��ͨ��','CrdTyp_ID','������','PK','�տ�','CARD_LVL','��ǿ�����','','10');
INSERT INTO MMAPST.ST_ITEM VALUES('LQNC','��Ⱥ��ͨ��','CrdTyp_ID','������','PK','�տ�','CARD_LVL','��ǿ�����','','10');
INSERT INTO MMAPST.ST_ITEM VALUES('QQCC','�����ഺ��','CrdTyp_ID','������','PK','�տ�','CARD_LVL','��ǿ�����','','10');
INSERT INTO MMAPST.ST_ITEM VALUES('QQDC','�������','CrdTyp_ID','������','PK','�տ�','CARD_LVL','��ǿ�����','','10');
INSERT INTO MMAPST.ST_ITEM VALUES('SIXE','6+E��','CrdTyp_ID','������','PK','�տ�','CARD_LVL','��ǿ�����','','10');
INSERT INTO MMAPST.ST_ITEM VALUES('SOCS','�籣���ڿ�','CrdTyp_ID','������','PK','�տ�','CARD_LVL','��ǿ�����','','10');
INSERT INTO MMAPST.ST_ITEM VALUES('SSYC','��԰��','CrdTyp_ID','������','PK','�տ�','CARD_LVL','��ǿ�����','','10');
INSERT INTO MMAPST.ST_ITEM VALUES('TSMC','TSM�����ֽ�','CrdTyp_ID','������','PK','�տ�','CARD_LVL','��ǿ�����','','10');
INSERT INTO MMAPST.ST_ITEM VALUES('VCRD','������','CrdTyp_ID','������','PK','�տ�','CARD_LVL','��ǿ�����','','10');
INSERT INTO MMAPST.ST_ITEM VALUES('WCRD','΢����','CrdTyp_ID','������','PK','�տ�','CARD_LVL','��ǿ�����','','10');
INSERT INTO MMAPST.ST_ITEM VALUES('YKTC','������ͨ��','CrdTyp_ID','������','PK','�տ�','CARD_LVL','��ǿ�����','','10');
INSERT INTO MMAPST.ST_ITEM VALUES('NOGC','��','CrdTyp_ID','������','JK','��','CARD_LVL','��ǿ�����','','20');
INSERT INTO MMAPST.ST_ITEM VALUES('YKTG','���ڽ�','CrdTyp_ID','������','JK','��','CARD_LVL','��ǿ�����','','20');
INSERT INTO MMAPST.ST_ITEM VALUES('LQGC','��Ⱥ��','CrdTyp_ID','������','JK','��','CARD_LVL','��ǿ�����','','20');
INSERT INTO MMAPST.ST_ITEM VALUES('YKTP','���ڰ׽�','CrdTyp_ID','������','BJK','�׽�','CARD_LVL','��ǿ�����','','30');
INSERT INTO MMAPST.ST_ITEM VALUES('LQPC','��Ⱥ�׽�','CrdTyp_ID','������','BJK','�׽�','CARD_LVL','��ǿ�����','','30');
INSERT INTO MMAPST.ST_ITEM VALUES('NOPC','�׽�','CrdTyp_ID','������','BJK','�׽�','CARD_LVL','��ǿ�����','','30');
INSERT INTO MMAPST.ST_ITEM VALUES('DCRD','������ʯ��','CrdTyp_ID','������','ZSK','��ʯ��','CARD_LVL','��ǿ�����','','40');
INSERT INTO MMAPST.ST_ITEM VALUES('CCRD','���ڲƸ���','CrdTyp_ID','������','CFK','�Ƹ���','CARD_LVL','��ǿ�����','','50');
INSERT INTO MMAPST.ST_ITEM VALUES('PVBC','����˽�����п�','CrdTyp_ID','������','SRYH','˽������','CARD_LVL','��ǿ�����','','60');
commit;
select * from   mmapst.ST_ITEM;
