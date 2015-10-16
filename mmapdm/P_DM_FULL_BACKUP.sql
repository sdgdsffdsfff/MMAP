CREATE OR REPLACE PROCEDURE "MMAPDM"."P_DM_FULL_BACKUP"
(
  DM_YESTERDAY IN INTEGER, --"上日"日期
  TABLE_NAME IN VARCHAR2, -- 表名
  IO_ROW OUT INTEGER  --插入条数
)
AS

  DM_SQL VARCHAR2(20000); -- 存放SQL语句
  ST_ROW INTEGER;  --源数据条数

BEGIN
  IO_ROW := 0;
  --备份"上日"数据
  --查询DM层表中是否有"上一日"数据，是否为重跑。
  DM_SQL := ' SELECT COUNT(1) FROM MMAPDM.'||TABLE_NAME||' WHERE PERIOD_ID='||DM_YESTERDAY;
  EXECUTE  IMMEDIATE DM_SQL  INTO ST_ROW;
  IF ST_ROW>0
  THEN
    DM_SQL :='TRUNCATE TABLE MMAPDM.'||TABLE_NAME||'_PRE' ;   -- 删除备份表中数据
    EXECUTE IMMEDIATE DM_SQL;
    COMMIT;
    DM_SQL :='INSERT INTO MMAPDM.'||TABLE_NAME||'_PRE SELECT * FROM MMAPDM.'||TABLE_NAME;   -- 备份表中数据
    EXECUTE IMMEDIATE DM_SQL;
    IO_ROW := SQL%ROWCOUNT ;
    COMMIT;
  END IF;

END P_DM_FULL_BACKUP;
