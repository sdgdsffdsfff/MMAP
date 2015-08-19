insert into mmapst.st_system_date
(
TX_DATE
,PRE_DATE
,ETL_DATE
,PRE_ETL_DATE
)
select 
to_number(to_char(TX_DT,'YYYYMMDD'))
,to_number(to_char(TX_DT-1,'YYYYMMDD'))
,to_number(to_char(SYSDATE,'YYYYMMDD'))
,to_number(to_char(SYSDATE-1,'YYYYMMDD'))
from mmapst.DMMKT_V_zSystem 
