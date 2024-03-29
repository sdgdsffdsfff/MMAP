CREATE OR REPLACE PROCEDURE "MMAPDM"."P_MMAPDM_CREATE_TMP_TABLES"(
	TABLE_NAME IN VARCHAR2,
	IO_STATUS OUT INTEGER ,
	VO_SQLERR OUT VARCHAR2,
	TMP_PRE OUT VARCHAR,
	TMP_CUR OUT VARCHAR,
	TMP_INS OUT VARCHAR,
	TMP_UPD OUT VARCHAR
)
IS
	STR VARCHAR2(300);
	STR_PRE VARCHAR2(300);
	STR_AUR VARCHAR2(300);
	STR_INS VARCHAR2(300);
	STR_UPD VARCHAR2(300);
	STRSQL VARCHAR2(800);
	NUM NUMBER(16);
	tmp_name	VARCHAR(35);
	IO_ROW INTEGER;

BEGIN
	IO_ROW := 0;
	IF	LENGTH(TABLE_NAME) > 22		-- 控制表名长度
		THEN	tmp_name := substr(TABLE_NAME,0,20);
		ELSE	tmp_name := TABLE_NAME;
	END IF;
	TMP_PRE :=	'TMP_' ||  tmp_name	 ||	'_PRE';
	TMP_CUR :=	'TMP_' ||  tmp_name	 ||	'_CUR';
	TMP_INS :=	'TMP_' ||  tmp_name	 ||	'_INS';
	TMP_UPD :=	'TMP_' ||  tmp_name	 ||	'_UPD';


	--判断临时表是否存在,存在则删除表
	STRSQL := 'SELECT COUNT(1) FROM USER_TABLES WHERE TABLE_NAME = '''|| TMP_PRE || '''';
	EXECUTE	IMMEDIATE STRSQL  INTO NUM;
  IF NUM > 0 THEN
    STRSQL := 'DROP TABLE ' || TMP_PRE;
    EXECUTE  IMMEDIATE STRSQL;
  END IF;

  STRSQL := 'SELECT COUNT(1) FROM USER_TABLES WHERE TABLE_NAME = ''' || TMP_CUR || '''';
  EXECUTE  IMMEDIATE STRSQL  INTO NUM;
  IF NUM > 0 THEN
    STRSQL := 'DROP TABLE ' || TMP_CUR;
    EXECUTE  IMMEDIATE STRSQL;
  END IF;

  STRSQL := 'SELECT COUNT(1) FROM USER_TABLES WHERE TABLE_NAME = ''' || TMP_INS || '''';
  EXECUTE  IMMEDIATE STRSQL  INTO NUM;
  IF NUM > 0 THEN
    STRSQL := 'DROP TABLE ' || TMP_INS;
    EXECUTE  IMMEDIATE STRSQL;
  END IF;

  STRSQL := 'SELECT COUNT(1) FROM USER_TABLES WHERE TABLE_NAME = ''' || TMP_UPD || '''';
  EXECUTE  IMMEDIATE STRSQL  INTO NUM;
  IF NUM > 0 THEN
    STRSQL := 'DROP TABLE ' || TMP_UPD;
    EXECUTE  IMMEDIATE  STRSQL;
  END IF;

  --创建临时表
  STR_PRE := 'CREATE TABLE MMAPDM.' || TMP_PRE || ' AS SELECT * FROM MMAPDM.' || TABLE_NAME || ' WHERE 1 = 2' ;
  STR_AUR := 'CREATE TABLE MMAPDM.' || TMP_CUR || ' AS SELECT * FROM MMAPDM.' || TABLE_NAME || ' WHERE 1 = 2' ;
  STR_INS := 'CREATE TABLE MMAPDM.' || TMP_INS || ' AS SELECT * FROM MMAPDM.' || TABLE_NAME || ' WHERE 1 = 2' ;
  STR_UPD := 'CREATE TABLE MMAPDM.' || TMP_UPD || ' AS SELECT * FROM MMAPDM.' || TABLE_NAME || ' WHERE 1 = 2' ;

  STR := STR_PRE;
  EXECUTE IMMEDIATE STR_PRE;
  STR := STR_AUR;
  EXECUTE IMMEDIATE STR_AUR;
  STR := STR_INS;
  EXECUTE IMMEDIATE STR_INS;
  STR := STR_UPD;
  EXECUTE IMMEDIATE STR_UPD;

  IO_STATUS :=  0 ;
  VO_SQLERR :=  'SUSSCESS';

  COMMIT;
  EXCEPTION WHEN OTHERS THEN
    ROLLBACK ;
    IO_STATUS := 9 ;
    VO_SQLERR := STRSQL  || CHR(10) || STR || SQLCODE ||  SQLERRM  ;
    P_MMAPDM_WRITE_LOGS(TABLE_NAME,IO_STATUS,IO_ROW,SYSDATE,SYSDATE,VO_SQLERR);
END  P_MMAPDM_CREATE_TMP_TABLES;
