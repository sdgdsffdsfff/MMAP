DROP TABLE MMAPDM.DM_CUSTOMER;
CREATE TABLE MMAPDM.DM_CUSTOMER
(
 ETL_DATE NUMBER(8)                 
,TX_DATE DATE
,PERIOD_ID NUMBER(8)                    
,CUSTOMER_ID VARCHAR2(50) NOT NULL
,CUST_NAME VARCHAR2(50)           
,AGE NUMBER(3)                   
,BIRTH_DT DATE 
,GENDER_CODE VARCHAR2(50)             
,GENDER VARCHAR2(50)
,EDU_CODE VARCHAR2(50)          
,EDUCATION VARCHAR2(50)
,MARITALSTS_CODE VARCHAR2(50)         
,MARITALSTS VARCHAR2(50)          
,FAMILY_GROUP_ID VARCHAR2(50)     
,CMP_DEP VARCHAR2(100)
,OCUP_CODE VARCHAR2(50)           
,OCUP VARCHAR2(50) 
,OCCP_CODE VARCHAR2(50)           
,OCCP VARCHAR2(50) 
,POSITION_CODE VARCHAR2(50)               
,POSITION VARCHAR2(50)  
,POSITION_TITLE_CODE VARCHAR2(50)        
,POSITION_TITLE VARCHAR2(50)      
,INCOME NUMBER(20,8)     
,INCOME_RANGE NUMBER(16) 
,RESD_CODE  VARCHAR2(50)     
,RESD VARCHAR2(50) 
,COUNTRY_CODE VARCHAR2(50)            
,COUNTRY VARCHAR2(50)
,RACE_CODE VARCHAR2(50)            
,RACE VARCHAR2(50)  
,IDTYPE_CODE VARCHAR2(50)          
,IDTYPE VARCHAR2(50)              
,IDCODE VARCHAR2(50) 
,IDCOUNTRY_CODE  VARCHAR2(50)           
,IDCOUNTRY VARCHAR2(50)           
,IDEXP_DT DATE                   
,IDISS_DT DATE                   
,CUST_BRANCH VARCHAR2(50)         
,OPEN_BRANCH VARCHAR2(50)         
,OWN_BRANCH VARCHAR2(50)         
,FIN_MANAGER VARCHAR2(50)         
,FIN_MANAGER_PHONE VARCHAR2(50)  
,CIFOPEN_DT date         
,CIFCLOSE_DT date
,CIF_LVL_CODE   VARCHAR2(50)      
,CIF_LVL VARCHAR2(50)             
,CIF_LVL_DATE DATE  
,CARD_LVL_CODE  VARCHAR2(50)           
,CARD_LVL VARCHAR2(50)            
,CARD_DATE DATE  
,RISK_LVL_CODE  VARCHAR2(50)            
,RISK_LVL_TYP VARCHAR2(50)
,RATING_LVL_CODE  VARCHAR2(50)     
,RATING_LVL VARCHAR2(50)          
,LOAN_AMT NUMBER(20,8)            
,CONTRIBUTION_RATE NUMBER(20,8)   
,CUST_LTV NUMBER(16)             
,EMP_IND number(16,0)            
,BLACKLIST_IND NUMBER(16)       
,WHITELIST_IND NUMBER(16)        
,BREAK_IND NUMBER(16)           
,CUST_STATUS_IND NUMBER(16)
,PRODUCT_HOLDING_NOW number(16,0)	                
,PRODUCT_HOLDING_HIS number(16,0)	
,CUST_LC_CD_BAL number(20,8)	
,CUST_LC_TD_BAL number(20,8)	
,CUST_LC_LOAN_BAL number(20,8)	
,CUST_LC_NDEBT_BAL number(20,8)	
,CUST_LC_FOUND_BAL number(20,8)	
,CUST_LC_FIN_BAL number(20,8)	
,CUST_LC_INSURE_BAL number(20,8)	
,CUST_LC_NOBLE_BAL number(20,8)	
,CUST_LC_CREDIT_BAL number(20,8)	
,CUST_LC_ALL_BAL number(20,8)	
,CUST_FC_CD_BAL number(20,8)	
,CUST_FC_TD_BAL number(20,8)	
,CUST_FC_LOAN_BAL number(20,8)	
,CUST_FC_CREDIT_BAL number(20,8)	
,CUST_FC_ALL_BAL number(20,8)	
,CUST_ALL_BAL number(20,8)	
,PRIMARY KEY (CUSTOMER_ID)
 );
DELETE FROM MMAPDM.DM_CUSTOMER;
INSERT INTO MMAPDM.DM_CUSTOMER
(
 ETL_DATE 
,TX_DATE
,PERIOD_ID
,CUSTOMER_ID
,CUST_NAME
,AGE
,BIRTH_DT
,GENDER_CODE  
,GENDER
,EDU_CODE
,EDUCATION
,MARITALSTS_CODE
,MARITALSTS
,Family_GROUP_ID
,CMP_DEP
,OCUP_CODE
,OCUP
,OCCP_CODE
,OCCP
,POSITION_CODE
,POSITION
,POSITION_TITLE_CODE 
,POSITION_TITLE
,INCOME
,INCOME_RANGE
,RESD_CODE
,RESD
,COUNTRY_CODE 
,COUNTRY
,RACE_CODE 
,RACE
,IDTYPE_CODE
,IDTYPE
,IDCODE
,IDCOUNTRY_CODE 
,IDCOUNTRY
,IDEXP_DT
,IDISS_DT
,CUST_BRANCH
,OPEN_BRANCH
,OWN_BRANCH
,FIN_MANAGER
,FIN_MANAGER_PHONE
,CIFOPEN_DT
,CIFCLOSE_DT
,CIF_LVL_CODE 
,CIF_LVL
,CIF_LVL_DATE
,CARD_LVL_CODE
,CARD_LVL
,CARD_Date
,RISK_LVL_CODE 
,RISK_LVL_TYP
,RATING_LVL_CODE 
,RATING_LVL
,LOAN_AMT
,CONTRIBUTION_RATE
,CUST_LTV
,EMP_IND
,BLACKLIST_IND
,WHITELIST_IND
,BREAK_IND
,CUST_STATUS_IND
,PRODUCT_HOLDING_NOW                
,PRODUCT_HOLDING_HIS
,CUST_LC_CD_BAL
,CUST_LC_TD_BAL
,CUST_LC_LOAN_BAL
,CUST_LC_NDEBT_BAL
,CUST_LC_FOUND_BAL
,CUST_LC_FIN_BAL
,CUST_LC_INSURE_BAL
,CUST_LC_NOBLE_BAL
,CUST_LC_CREDIT_BAL
,CUST_LC_ALL_BAL
,CUST_FC_CD_BAL
,CUST_FC_TD_BAL
,CUST_FC_LOAN_BAL
,CUST_FC_CREDIT_BAL
,CUST_FC_ALL_BAL
,CUST_ALL_BAL
)
SELECT
 TO_NUMBER(TO_CHAR(SYSDATE,'YYYYMMDD')) AS ETL_DATE                
,TX_DATE
,TO_NUMBER(TO_CHAR(SYSDATE,'YYYYMMDD')) AS PERIOD_ID                 
,CUSTOMER_ID             
,CUST_NAME               
,AGE                     
,BIRTH_DT 
,GENDER_CODE               
,B.ITEM_NM AS GENDER
,EDU_CODE          
,C.ITEM_NM AS EDUCATION 
,MARITALSTS_CODE      
,D.ITEM_NM AS MARITALSTS      
,NULL AS FAMILY_GROUP_ID 
,CMP_DEP 
,OCUP_CODE                
,E.ITEM_NM AS OCUP  
,OCCP_CODE          
,F.ITEM_NM AS OCCP  
,POSITION_CODE          
,G.ITEM_NM AS POSITION  
,POSITION_TITLE_CODE      
,H.ITEM_NM AS POSITION_TITLE  
,INCOME                  
,INCOME_RANGE 
,RESD_CODE           
,I.ITEM_NM AS  RESD 
,COUNTRY_CODE          
,J.ITEM_NM AS COUNTRY
,RACE_CODE         
,K.ITEM_NM AS RACE            
,IDTYPE_CODE
,L.ITEM_NM AS IDTYPE            
,IDCODE                  
,IDCOUNTRY_CODE
,M.ITEM_ID AS  IDCOUNTRY       
,IDEXP_DT                
,IDISS_DT                
,CUST_BRANCH             
,OPEN_BRANCH             
,OWN_BRANCH              
,FIN_MANAGER             
,FIN_MANAGER_PHONE       
,CIFOPEN_DT              
,CIFCLOSE_DT 
,CIF_LVL_CODE            
,N.ITEM_NM AS CIF_LVL         
,NULL AS CIF_LVL_DATE 
,NULL AS CARD_LVL_CODE 
,NULL AS CARD_LVL       
,NULL AS CARD_DATE
,RISK_LVL_CODE      
,O.ITEM_NM AS RISK_LVL_TYP  
,RATING_LVL_CODE  
,P.ITEM_NM AS RATING_LVL      
,LOAN_AMT                
,CONTRIBUTION_RATE       
,NULL AS CUST_LTV        
,EMP_IND                 
,BLACKLIST_IND           
,WHITELIST_IND           
,NULL AS BREAK_IND       
,NULL AS CUST_STATUS_IND 
,NULL AS PRODUCT_HOLDING_NOW                
,NULL AS PRODUCT_HOLDING_HIS
,NULL AS CUST_LC_CD_BAL
,NULL AS CUST_LC_TD_BAL
,NULL AS CUST_LC_LOAN_BAL
,NULL AS CUST_LC_NDEBT_BAL
,NULL AS CUST_LC_FOUND_BAL
,NULL AS CUST_LC_FIN_BAL
,NULL AS CUST_LC_INSURE_BAL
,NULL AS CUST_LC_NOBLE_BAL
,NULL AS CUST_LC_CREDIT_BAL
,NULL AS CUST_LC_ALL_BAL
,NULL AS CUST_FC_CD_BAL
,NULL AS CUST_FC_TD_BAL
,NULL AS CUST_FC_LOAN_BAL
,NULL AS CUST_FC_CREDIT_BAL
,NULL AS CUST_FC_ALL_BAL
,NULL AS CUST_ALL_BAL
FROM MMAPST.ST_CUSTOMER A
LEFT JOIN (SELECT ITEM_ID,ITEM_NM FROM MMAPST.ST_ITEM  WHERE ITEM_TYP='GENDER')B
ON A.GENDER_CODE=B.ITEM_ID
LEFT JOIN (SELECT ITEM_ID,ITEM_NM FROM MMAPST.ST_ITEM  WHERE ITEM_TYP='EDU')C
ON A.EDU_CODE=C.ITEM_ID
LEFT JOIN (SELECT ITEM_ID,ITEM_NM FROM MMAPST.ST_ITEM  WHERE ITEM_TYP='MARITALSTS')D
ON A.MARITALSTS_CODE=D.ITEM_ID
LEFT JOIN (SELECT ITEM_ID,ITEM_NM FROM MMAPST.ST_ITEM  WHERE ITEM_TYP='OCUP')E
ON A.OCUP_CODE=E.ITEM_ID
LEFT JOIN (SELECT ITEM_ID,ITEM_NM FROM MMAPST.ST_ITEM  WHERE ITEM_TYP='OCCP')F
ON A.OCCP_CODE=F.ITEM_ID
LEFT JOIN (SELECT ITEM_ID,ITEM_NM FROM MMAPST.ST_ITEM  WHERE ITEM_TYP='POSITION')G
ON A.POSITION_CODE=G.ITEM_ID
LEFT JOIN (SELECT ITEM_ID,ITEM_NM FROM MMAPST.ST_ITEM  WHERE ITEM_TYP='POSITION_TITLE')H
ON A.POSITION_TITLE_CODE=H.ITEM_ID
LEFT JOIN (SELECT ITEM_ID,ITEM_NM FROM MMAPST.ST_ITEM  WHERE ITEM_TYP='RESD')I
ON A.RESD_CODE=I.ITEM_ID
LEFT JOIN (SELECT ITEM_ID,ITEM_NM FROM MMAPST.ST_ITEM  WHERE ITEM_TYP='COUNTRY')J
ON A.COUNTRY_CODE=J.ITEM_ID
LEFT JOIN (SELECT ITEM_ID,ITEM_NM FROM MMAPST.ST_ITEM  WHERE ITEM_TYP='RACE')K
ON A.RACE_CODE=K.ITEM_ID
LEFT JOIN (SELECT ITEM_ID,ITEM_NM FROM MMAPST.ST_ITEM  WHERE ITEM_TYP='IDTYPE')L
ON A.IDTYPE_CODE=L.ITEM_ID
LEFT JOIN (SELECT ITEM_ID,ITEM_NM FROM MMAPST.ST_ITEM  WHERE ITEM_TYP='IDCOUNTRY')M
ON A.IDCOUNTRY_CODE=M.ITEM_ID
LEFT JOIN (SELECT ITEM_ID,ITEM_NM FROM MMAPST.ST_ITEM  WHERE ITEM_TYP='CIF_LVL')N
ON A.CIF_LVL_CODE=N.ITEM_ID
LEFT JOIN (SELECT ITEM_ID,ITEM_NM FROM MMAPST.ST_ITEM  WHERE ITEM_TYP='RISK_LVL')O
ON A.RISK_LVL_CODE=O.ITEM_ID
LEFT JOIN (SELECT ITEM_ID,ITEM_NM FROM MMAPST.ST_ITEM  WHERE ITEM_TYP='RATING_LVL')P
ON A.RATING_LVL_CODE=P.ITEM_ID;
COMMIT;


