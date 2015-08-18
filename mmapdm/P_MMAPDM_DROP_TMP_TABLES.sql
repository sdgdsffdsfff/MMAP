create or replace procedure P_MMAPDM_DROP_TMP_TABLES(
       TABLE_NAME IN VARCHAR2,
       IO_STATUS OUT INTEGER ,
       VO_SQLERR OUT VARCHAR2
) 
IS     
       STR VARCHAR2(300);
       STR_PRE VARCHAR2(300);
       STR_AUR VARCHAR2(300);
       STR_INS VARCHAR2(300);
       STR_UPD VARCHAR2(300);
       tmp_name VARCHAR(35);
       IO_ROW INTEGER;
BEGIN
       IF LENGTH(TABLE_NAME) > 22 THEN
          tmp_name := substr(TABLE_NAME, 0, 20);
       ELSE
          tmp_name := TABLE_NAME;
       END IF;
       
       STR_PRE := 'DROP TABLE MMAPDM.TMP_' ||  trim(tmp_name)  || '_PRE' ;
       STR_AUR := 'DROP TABLE MMAPDM.TMP_' ||  trim(tmp_name)  || '_CUR' ;
       STR_INS := 'DROP TABLE MMAPDM.TMP_' ||  trim(tmp_name)  || '_INS' ;
       STR_UPD := 'DROP TABLE MMAPDM.TMP_' ||  trim(tmp_name)  || '_UPD' ;
       
       
       STR := STR_PRE;
       EXECUTE IMMEDIATE STR_PRE;
       STR := STR_AUR;
       EXECUTE IMMEDIATE STR_AUR;
       STR := STR_INS;
       EXECUTE IMMEDIATE STR_INS;
       STR := STR_UPD;
       EXECUTE IMMEDIATE STR_UPD;

       IO_STATUS := 0 ;
       VO_SQLERR := 'SUSSCESS';

       COMMIT;
    EXCEPTION WHEN OTHERS THEN
        ROLLBACK ; 
        IO_STATUS := 9 ;
        VO_SQLERR := STR || SQLCODE || SQLERRM ;
        P_MMAPDM_WRITE_LOGS(TABLE_NAME,IO_STATUS,IO_ROW,SYSDATE,SYSDATE,VO_SQLERR);
end P_MMAPDM_DROP_TMP_TABLES;
/
