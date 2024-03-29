CREATE OR REPLACE PROCEDURE "MMAPDM"."P_MMAPDM_WRITE_LOGS"(
       TRANSNAME IN VARCHAR2,
       STATUS IN  VARCHAR2,
       IO_ROW IN INTEGER,
       STARTDATE IN DATE,
       ENDDATE IN DATE,
       LOG_FIELD IN CLOB
) IS
BEGIN
   insert into MMAPDM_TRANS_LOG values (TRANSNAME,STATUS,IO_ROW,STARTDATE,ENDDATE,LOG_FIELD);
   COMMIT;
end P_MMAPDM_WRITE_LOGS;
