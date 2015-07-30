create or replace procedure P_MID_CALENDAR(start_date in char, end_date in char) as

  v_counter number := 0;

  v_max number := 0;

  v_date date;

begin

  execute immediate 'truncate table MID_CALENDAR';

  v_max := to_number(TO_DATE(end_date, 'yyyy-mm-dd')-TO_DATE(start_date, 'yyyy-mm-dd'));

  loop
     v_date := TO_DATE(start_date, 'yyyy-mm-dd')+v_counter;
     insert into MMAPST.MID_CALENDAR
     (
       PERIOD_ID
       ,PERIOD
       ,YEAR
       ,QUARTER
       ,MONTH
       ,WEEKOFYEAR
       ,DAYOFYEAR
       ,DAYOFQUARTER
       ,DAYOFMONTH
       ,DAYOFWEEK
       ,YEAR_END_SIGN
       ,QUARTER_END_SIGN
       ,MONTH_END_SIGN
       ,WEEK_END_SIGN
     )
     values (
       TO_NUMBER(TO_CHAR(v_date,'YYYYMMDD'))
       ,v_date
       ,TO_NUMBER(to_char(v_date,'yyyy'))
       ,TO_NUMBER(to_char(v_date,'Q'))
       ,TO_NUMBER(to_char(v_date,'MM'))
       ,TO_NUMBER(to_char(v_date,'WW'))
       ,TO_NUMBER(to_char(v_date,'DDD'))
       ,TO_NUMBER(to_char(v_date,'DDD'))-TO_NUMBER(to_char(trunc(v_date,'Q'),'DDD'))+1
       ,TO_NUMBER(to_char(v_date,'DD'))
       ,TO_NUMBER(to_char(v_date,'D'))
       ,CASE WHEN TO_DATE(to_char(v_date,'YYYYMMDD'),'YYYY-MM-DD')=trunc(ADD_MONTHS(v_date,12),'Y')-1 THEN 1 ELSE 0 END
       ,CASE WHEN TO_DATE(to_char(v_date,'YYYYMMDD'),'YYYY-MM-DD')=trunc(ADD_MONTHS(v_date,3),'Q')-1 THEN 1 ELSE 0 END
       ,CASE WHEN v_date=LAST_DAY(v_date) THEN 1 ELSE 0 END
       ,CASE WHEN TO_NUMBER(to_char(v_date,'D'))=7 THEN 1 ELSE 0 END
     );

     exit when  v_counter >= v_max;

     v_counter := v_counter+1;

   end loop;

   commit;

end P_MID_CALENDAR;
