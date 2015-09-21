/*
目标表：MID_CALENDAR
源表：
开发人:	郭盼盼
开日期:	20150731
特殊说明:
	1.调度该存储过程方式，以加载2010年至2025年数据为例: CALL P_MID_CALENDAR('20100101','20251231')
变更记录:
*/
CREATE OR REPLACE PROCEDURE P_MID_CALENDAR(
start_date IN CHAR,
end_date IN CHAR
) AS

v_counter number := 0;

v_max number :=	0;

v_date DATE;

BEGIN

	EXECUTE	IMMEDIATE 'TRUNCATE TABLE MID_CALENDAR';

	v_max	:= to_number(TO_DATE(end_date, 'yyyy-mm-dd')-TO_DATE(start_date, 'yyyy-mm-dd'));

	LOOP
		v_date := TO_DATE(start_date, 'yyyy-mm-dd')+v_counter;
		INSERT INTO MMAPST.MID_CALENDAR
		(
			PERIOD_ID		--日期ID
			,PERIOD			--日期
			,YEAR			--年
			,QUARTER		--季
			,MONTH			--月
			,WEEKOFYEAR		--周/年
			,DAYOFYEAR		--日/年
			,DAYOFQUARTER		--日/季
			,DAYOFMONTH		--日/月
			,DAYOFWEEK		--日/周
			,YEAR_END_SIGN		--年末标识
			,QUARTER_END_SIGN	--季末标识
			,MONTH_END_SIGN		--月末标识
			,WEEK_END_SIGN		--周末标识
		)
		VALUES
		(
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
			,CASE WHEN v_date=LAST_DAY(v_date) THEN	1 ELSE 0 END
			,CASE WHEN TO_NUMBER(to_char(v_date,'D'))=7 THEN 1 ELSE 0 END
		);

		EXIT WHEN v_counter >= v_max;

		v_counter := v_counter+1;

	END LOOP;

	COMMIT;

END P_MID_CALENDAR;
