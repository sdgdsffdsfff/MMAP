CREATE OR REPLACE PROCEDURE "MMAPDM"."P_DM_FULL_ROLLBACK"
(
	TABLE_NAME IN VARCHAR2, -- 表名
	IO_ROW OUT INTEGER  --插入条数
)
AS
	DM_SQL VARCHAR2(20000); -- 存放SQL语句
  
  V_COUNT NUMBER;  --计数变量
  V_COMMITNUM CONSTANT NUMBER :=500000;--一次提交记录数（默认一百万）
  
  --定义一个游标数据类型  
  TYPE emp_cursor_type IS REF CURSOR;  
  --声明一个游标变量  
  c1 EMP_CURSOR_TYPE;  
  --声明记录变量  
  V_DM_BRANCH DM_BRANCH%ROWTYPE;
  V_DM_CUSTOMER DM_CUSTOMER%ROWTYPE;
  V_DM_PRODUCT DM_PRODUCT%ROWTYPE;
  V_DM_ACCOUNT_DEP DM_ACCOUNT_DEP%ROWTYPE;
  V_DM_ACCOUNT_LOAN DM_ACCOUNT_LOAN%ROWTYPE;
  V_DM_CUST_RELATION DM_CUST_RELATION%ROWTYPE;
  V_DM_CUST_CONTACT DM_CUST_CONTACT%ROWTYPE;
  V_DM_CUST_AGREEMENT DM_CUST_AGREEMENT%ROWTYPE;
  V_DM_ACCOUNT_INVEST DM_ACCOUNT_INVEST%ROWTYPE;
  V_DM_ACCOUNT DM_ACCOUNT%ROWTYPE;
  V_DM_CUST_CARD DM_CUST_CARD%ROWTYPE;
  V_DM_CUST_STATIC_IND DM_CUST_STATIC_IND%ROWTYPE;
BEGIN
  IO_ROW := 0;
  -- 删除目标表中数据
  DM_SQL :='TRUNCATE TABLE MMAPDM.'||TABLE_NAME;   
  EXECUTE IMMEDIATE DM_SQL;
  COMMIT;
  --恢复备份中数据
  DM_SQL :=' SELECT * FROM MMAPDM.'||TABLE_NAME||'_PRE';
  
    --计数器初始化 
    V_COUNT  := 0;  
   
   --批量插入当日数据
    OPEN C1 FOR DM_SQL;
      LOOP
        --确定取数SQL
        IF TABLE_NAME='DM_BRANCH'
        THEN
          FETCH C1 INTO V_DM_BRANCH;
          EXIT WHEN C1%NOTFOUND;
          INSERT INTO DM_BRANCH VALUES V_DM_BRANCH;
          IO_ROW := IO_ROW+SQL%ROWCOUNT ;
          
        ELSIF TABLE_NAME='DM_CUSTOMER'
        THEN
          FETCH C1 INTO V_DM_CUSTOMER;
          EXIT WHEN C1%NOTFOUND;
          INSERT INTO DM_CUSTOMER VALUES V_DM_CUSTOMER;
          IO_ROW := IO_ROW+SQL%ROWCOUNT ;
          
        ELSIF TABLE_NAME='DM_PRODUCT'
        THEN
          FETCH C1 INTO V_DM_PRODUCT;
          EXIT WHEN C1%NOTFOUND;
          INSERT INTO DM_PRODUCT VALUES V_DM_PRODUCT;
          IO_ROW := IO_ROW+SQL%ROWCOUNT ;
          
        ELSIF TABLE_NAME='DM_ACCOUNT_DEP'
        THEN
          FETCH C1 INTO V_DM_ACCOUNT_DEP;
          EXIT WHEN C1%NOTFOUND;
          INSERT INTO DM_ACCOUNT_DEP VALUES V_DM_ACCOUNT_DEP;
          IO_ROW := IO_ROW+SQL%ROWCOUNT ;
          
        ELSIF TABLE_NAME='DM_ACCOUNT_LOAN'
        THEN
          FETCH C1 INTO V_DM_ACCOUNT_LOAN;
          EXIT WHEN C1%NOTFOUND;
          INSERT INTO DM_ACCOUNT_LOAN VALUES V_DM_ACCOUNT_LOAN;
          IO_ROW := IO_ROW+SQL%ROWCOUNT ;
          
        ELSIF TABLE_NAME='DM_CUST_CARD'
        THEN
          FETCH C1 INTO V_DM_CUST_CARD;
          EXIT WHEN C1%NOTFOUND;
          INSERT INTO DM_CUST_CARD VALUES V_DM_CUST_CARD;
          IO_ROW := IO_ROW+SQL%ROWCOUNT ;
          
        ELSIF TABLE_NAME='DM_CUST_RELATION'
        THEN
          FETCH C1 INTO V_DM_CUST_RELATION;
          EXIT WHEN C1%NOTFOUND;
          INSERT INTO DM_CUST_RELATION VALUES V_DM_CUST_RELATION;
          IO_ROW := IO_ROW+SQL%ROWCOUNT ;
          
        ELSIF TABLE_NAME='DM_CUST_CONTACT'
        THEN
          FETCH C1 INTO V_DM_CUST_CONTACT;
          EXIT WHEN C1%NOTFOUND;
          INSERT INTO DM_CUST_CONTACT VALUES V_DM_CUST_CONTACT;
          IO_ROW := IO_ROW+SQL%ROWCOUNT ;
          
        ELSIF TABLE_NAME='DM_CUST_AGREEMENT'
        THEN
          FETCH C1 INTO V_DM_CUST_AGREEMENT;
          EXIT WHEN C1%NOTFOUND;
          INSERT INTO DM_CUST_AGREEMENT VALUES V_DM_CUST_AGREEMENT;
          IO_ROW := IO_ROW+SQL%ROWCOUNT ;
          
        ELSIF TABLE_NAME='DM_CUST_STATIC_IND'
        THEN
          FETCH C1 INTO V_DM_CUST_STATIC_IND;
          EXIT WHEN C1%NOTFOUND;
          INSERT INTO DM_CUST_STATIC_IND VALUES V_DM_CUST_STATIC_IND;
          IO_ROW := IO_ROW+SQL%ROWCOUNT ;
          
        ELSIF TABLE_NAME='DM_ACCOUNT_INVEST'
        THEN
          FETCH C1 INTO V_DM_ACCOUNT_INVEST;
          EXIT WHEN C1%NOTFOUND;
          INSERT INTO DM_ACCOUNT_INVEST VALUES V_DM_ACCOUNT_INVEST;
          IO_ROW := IO_ROW+SQL%ROWCOUNT ;
          
        ELSIF TABLE_NAME='DM_ACCOUNT'
        THEN
          FETCH C1 INTO V_DM_ACCOUNT;
          EXIT WHEN C1%NOTFOUND;
          INSERT INTO DM_ACCOUNT VALUES V_DM_ACCOUNT;
          IO_ROW := IO_ROW+SQL%ROWCOUNT ;
          
        ELSE EXIT;
        END IF;
        V_COUNT := V_COUNT + 1;
        IF ((MOD(V_COUNT, V_COMMITNUM)) = 0) THEN        
          COMMIT; 
        END IF;      
        
      END LOOP;
      COMMIT;
      CLOSE C1;

END P_DM_FULL_ROLLBACK;
