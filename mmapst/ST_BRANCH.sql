TRUNCATE TABLE MMAPST.ST_BRANCH;
INSERT INTO MMAPST.ST_BRANCH 
(
 ETL_DATE
,TX_DATE
,PERIOD_ID
,REGION_ID
,REGION_NM
,FBRNH_ID
,FBRNH_NM
,BRNH_ID
,BRNH_NM
,EQUIPMENT_ID
,CHANNEL
,TELEPHONE
,ADDR
,SOURCEGRP
) 

SELECT
 TO_NUMBER(TO_CHAR(SYSDATE,'YYYYMMDD')) AS ETL_DATE  
,B.TX_DT
,TO_NUMBER(TO_CHAR(B.TX_DT,'YYYYMMDD')) AS PERIOD_ID  
,SUBSTR(FBRNH_ID,1,2) AS REGION_ID
,CASE  WHEN SUBSTR(FBRNH_ID,1,2)='80' THEN '青岛地区'
        WHEN SUBSTR(FBRNH_ID,1,2)='81' THEN '济南地区'
        WHEN SUBSTR(FBRNH_ID,1,2)='82' THEN '东营地区'
        WHEN SUBSTR(FBRNH_ID,1,2)='83' THEN '威海地区'
        WHEN SUBSTR(FBRNH_ID,1,2)='85' THEN '淄博地区'
        WHEN SUBSTR(FBRNH_ID,1,2)='86' THEN '德州地区'
        WHEN SUBSTR(FBRNH_ID,1,2)='87' THEN '枣庄地区'
        WHEN SUBSTR(FBRNH_ID,1,2)='88' THEN '烟台地区'
END AS REGION_NM
,TRIM(FBRNH_ID) AS FBRNH_ID
,TRIM(FBRNH_NM) AS FBRNH_NM
,TRIM(BRNH_ID) AS BRNH_ID
,TRIM(BRNH_NM) AS BRNH_NM
,TRIM(EQUIPMENT_ID) AS EQUIPMENT_ID
,TRIM(CHANNEL) AS CHANNEL
,TRIM(TELEPHONE) AS TELEPHONE
,TRIM(ADDR) AS ADDR
,TRIM(SOURCEGRP) AS SOURCEGRP
FROM 
(
SELECT 
a.OBRNH_ID AS FBRNH_ID
,B.BRNH_NM AS FBRNH_NM
,a.BRNH_ID
,a.BRNH_NM
,NULL AS EQUIPMENT_ID
,'BRANCH' AS CHANNEL
,a.CNTCTEL_NO AS TELEPHONE
,a.ADDR AS ADDR
,'RLBRNH' AS SOURCEGRP
FROM MMAPDMMKT.DMMKT_RLBRNH a
LEFT JOIN MMAPDMMKT.DMMKT_RLBRNH b
ON a.OBRNH_ID=B.BRNH_ID
WHERE a.OBRNH_ID IS NOT NULL
AND a.BRNH_ID IS NOT NULL
AND a.BRNH_NM IS NOT NULL
AND a.BRNH_ID  NOT IN('80002' ,'0','90000','88000')
AND a.OBRNH_ID <> '80002'
UNION
SELECT DISTINCT a.BRNH_ID AS FBRNH_ID
,a.BRNH_NM AS FBRNH_NM
,a.BRNH_ID 
,a.BRNH_NM 
,NULL AS EQUIPMENT_ID
,'BRANCH' AS CHANNEL
,a.CNTCTEL_NO AS TELEPHONE
,a.ADDR AS ADDR
,'RLBRNH' AS SOURCEGRP   
FROM MMAPDMMKT.DMMKT_RLBRNH a
WHERE OBRNH_ID='80002' 
AND BRNH_ID IS NOT NULL
AND BRNH_NM IS NOT NULL
AND BRNH_ID NOT IN('80002' ,'0')
) a
LEFT JOIN MMAPDMMKT.DMMKT_V_ZSYSTEM B
ON 1=1
;
