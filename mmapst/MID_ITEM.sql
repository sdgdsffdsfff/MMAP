truncate table mmapst.ST_ITEM;
INSERT INTO MMAPST.ST_ITEM 

--�ͻ����յȼ�_����
select distinct a.item_id
       ,a.item_nm
       ,a.item_typ
       ,a.item_des
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
select distinct a.item_id
       ,a.item_nm
       ,a.item_typ
       ,a.item_des
       ,TRIM(a.item_id)
       ,CASE WHEN trim(a.item_id)='-' THEN 'δ֪' ELSE TRIM(a.item_nm) END
       ,'CIF_LVL'
       ,TRIM(a.item_des) 
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='CIF_Lvl' 
UNION ALL
--����
select distinct a.item_id
       ,a.item_nm
       ,a.item_typ
       ,a.item_des
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
select distinct a.item_id
       ,a.item_nm
       ,a.item_typ
       ,a.item_des
       ,a.item_id
       ,CASE WHEN trim(a.item_id)='-' THEN 'δ֪' ELSE TRIM(a.item_nm) END
       ,'COUNTRY'
       ,TRIM(a.item_des) 
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='Citizen_ID' 
UNION ALL
--ְ��
select distinct a.item_id
       ,a.item_nm
       ,a.item_typ
       ,a.item_des
       ,a.item_id
       ,CASE WHEN trim(a.item_id)='-' THEN 'δ֪' ELSE TRIM(a.item_nm) END
       ,'POSITION_TITLE'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='ProfTitl_ID' 
UNION ALL
--ְҵ
select distinct a.item_id
       ,a.item_nm
       ,a.item_typ
       ,a.item_des
       ,a.item_id
       ,CASE WHEN trim(a.item_id)='-' THEN 'δ֪' ELSE TRIM(a.item_nm) END
       ,'OCCP'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='Occp_ID'
UNION ALL 
--ְλ
select distinct a.item_id
       ,a.item_nm
       ,a.item_typ
       ,a.item_des
       ,a.item_id
       ,CASE WHEN trim(a.item_id)='-' THEN 'δ֪' ELSE TRIM(a.item_nm) END
       ,'POSITION'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='JobDes_ID' 
UNION ALL
--����״̬
select distinct a.item_id
       ,a.item_nm
       ,a.item_typ
       ,a.item_des
       ,a.item_id
       ,CASE WHEN trim(a.item_id)='-' THEN 'δ֪' ELSE TRIM(a.item_nm) END
       ,'MARITALSTS'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='MaritalSts_ID' 
UNION ALL
--�����̶�
select distinct a.item_id
       ,a.item_nm
       ,a.item_typ
       ,a.item_des
       ,a.item_id
       ,CASE WHEN trim(a.item_id)='-' THEN 'δ֪' ELSE TRIM(a.item_nm) END
       ,'EDU'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='Edu_ID' 
UNION ALL
--�Ա�
select distinct a.item_id
       ,a.item_nm
       ,a.item_typ
       ,a.item_des
       ,a.item_id
       ,CASE WHEN trim(a.item_id)='-' THEN 'δ֪' ELSE TRIM(a.item_nm) END
       ,'GENDER'
       ,TRIM(a.item_des)
       ,NULL
       ,NULL
from  mmapst.DMMKT_PCODEMAST a
WHERE ITEM_TYP ='Gender_ID' 
;
commit;
select * from   mmapst.ST_ITEM;
