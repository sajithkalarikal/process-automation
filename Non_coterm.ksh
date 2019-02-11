#!/bin/ksh
SCRIPTS='/appinfprd/bi/infogix/IA83/InfogixClient/scripts_edm/Dev/Remidationscripts/scripts'
LOG_PATH='/appinfprd/bi/infogix/IA83/InfogixClient/scripts_edm/Dev/Remidationscripts/Logs'
TICKET_TYPE=$1
TICKET_NO=$2
DIR_PATH='/appinfprd/bi/infogix/IA83/InfogixClient/scripts_edm/Dev/Remidationscripts/DRTfiles/Non_coterm'
Logfile=$LOG_PATH"Non_coterm"$(date +%Y%m%d_%T).log
CURRENT_DATE=$(date +%Y%m%d)
#DIR_NM='BTB_PDM_'$TICKET_NO'Non_coterm'$CURRENT_DATE


#create log
 
exec 1>>$Logfile 2>&1

#create file names based on input parameters BRAND -TRIGGER -REBRAND FINAL 
#DIR_FULL_NM="$DIR_PATH $TICKET_TYPE _$TICKET_NO _$DIR_NM_$CURRENT_DATE"
  
FILE_NM="DBT_Used_Sold_SP_COTERM_${CURRENT_DATE}.txt"
FILE_NM1="DBTB_Used_Inventory_SP_COTERM_${CURRENT_DATE}.txt"

#Change execution path/Non_coterm


cd $SCRIPTS ||

echo "Current execution path " $PWD
START_TIME=$(date +"%T")
echo 'Script execution begins at :' $START_TIME
bteq<<EOF
.LOGON tdprod/UP_EDWETL_SPC, pdm102113;



---Get latest transaction_ID
DROP TABLE  dp_wedw_snd.SP_COTERM_Extract;

CREATE TABLE dp_wedw_snd.SP_COTERM_Extract AS (
SEL ste.*  
FROM dp_vedw_pdm.score_trans_extract ste 
INNER JOIN dp_vedw_xms.v_StateEsnDimension sed
            ON ste.esn=sed.esn
WHERE sed.StatusCode = 'A'
AND Cast(receipt_ts AS DATE) BETWEEN DATE - 90 AND DATE - 2 
AND source_system = 'SIRI'
AND changedateend > DATE
QUALIFY Row_Number() Over (PARTITION BY ste.esn ORDER BY transactionid DESC) = 1
) WITH DATA PRIMARY INDEX(transactionid);

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE;

-----------------------------------------------------------------------
---New Sold Received indicator 
-----------------------------------------------------------------------
ALTER TABLE dp_wedw_snd.SP_COTERM_Extract
ADD New_Sold_Indicator CHAR(01) DEFAULT 'N' ;

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE;
----------------------------------------------------------------------------------------
--Remove all non remove all non-Sold/Recived records
-----------------------------------------------------------------------------------------
DELETE FROM dp_wedw_snd.SP_COTERM_Extract 
WHERE (eventname  NOT IN ('CPO Received Cert', 'Used Car Received'
													 ,'Used Car Sold', 'Used SaleCancelled'
													 , 'CPO Sold Cert','CPO SaleCancelled') 
			OR eventname IS NULL);
			
.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE;			
-----------------------------------------------------------------------
---Delete Non-Actionables
-----------------------------------------------------------------------
DELETE FROM  dp_wedw_snd.SP_COTERM_Extract 
WHERE (
				errmsg IS NULL 
	 			OR errmsg IN ('Event is duplicate','Event is a duplicate'
	 									,'Incoming event date is prior than last processed same event'
	 									,'USED/CPO RECEIVED event received with higher transaction id'
	 									,'USED/CPO RECEIVED event received with later event date')
				);

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE;
				
-----------------------------------------------------------------------
----Set records with new sold received
-----------------------------------------------------------------------

UPDATE dp_wedw_snd.SP_COTERM_Extract 
SET New_Sold_Indicator  = 'Y'
WHERE esn IN (SELECT DISTINCT esn   FROM DP_VEDW_BIZ_SPO.v_StateLifDimension
WHERE newSoldDate IS NOT NULL
AND lifstate = 'New_Sold');

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE;

/*SELECT * FROM dp_wedw_snd.SP_COTERM_Extract 
WHERE eventname LIKE '%received%' AND new_sold_indicator = 'N';*/

-----------------------------------------------------------------------
----remove exclusion list partners
-----------------------------------------------------------------------
DELETE  FROM  dp_wedw_snd.SP_COTERM_Extract  
 WHERE corpid  IN  (2524   -- BMW
                                 ,11901  -- Audi
                                 ,12158  -- Honda
                                 ,12162  -- GM
                                 ,12166) -- Hyundai
 AND new_sold_indicator = 'N';

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE; 
--------------------------------------------------------------------------
----remove GM AUTO_RETURNS - 2017-11-21
----------------------------------------------------------------------------
 DELETE  FROM  dp_wedw_snd.SP_COTERM_Extract
 WHERE corpid = 12162 
 AND eventname = 'CPO Received Cert';

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE;
 
 -----------------------------------------------------------------------
 ---Get latest plans
 -----------------------------------------------------------------------
DROP TABLE dp_wedw_snd.SP_COTERM_Current_Status;

CREATE TABLE  dp_wedw_snd.SP_COTERM_Current_Status AS ( 
WITH t1 AS
(
SELECT v.ESN,x.SERVICE_ID, x.status, multi_month_cycle_start_dt, multi_month_cycle_end_dt, pl.PLAN_NAME
FROM 
(
SELECT ACCS_METH_ID AS ESN, SERVICE_ID
FROM DP_VEDW_BIZ_XM.ACCS_METH_SERVICE_HIST
WHERE ACCS_METH_ID IN (SELECT esn FROM dp_wedw_snd.SP_COTERM_Extract) --esn plugin
AND ACCS_METH_TYPE_CD='/SERVICE/SIRIUS/ESN'
QUALIFY Row_Number()Over(PARTITION BY accs_meth_id ORDER BY accs_meth_service_end_dt DESC)=1
) v
INNER JOIN DP_VEDW_BIZ_XM.ACCT_PROD_SVC_XREF x ON v.SERVICE_ID=x.SERVICE_ID
AND multi_month_cycle_start_dt IS NOT NULL
LEFT JOIN DP_VEDW_BIZ_XM.PLAN pl ON x.PLAN_ID=pl.PLAN_ID
)

SELECT
t1.ESN,
t1.SERVICE_ID,
AUD.Aud_status,
AUD.Audio,
Aud_St_Dt,
Aud_End_dt,

Traf.traf_status,
Traf.Traffic,
Traf.Traf_St_Dt,
Traf.Traf_End_dt,

Trav.trav_status,
Trav.Travel,
Trav.Trav_St_Dt,
Trav.Trav_End_dt

FROM t1

LEFT JOIN
(SELECT ESN, status AS Aud_status, plan_name AS Audio, multi_month_cycle_start_dt AS Aud_St_Dt, multi_month_cycle_end_dt AS Aud_End_dt  FROM t1 WHERE plan_name NOT LIKE '%traf%' AND plan_name NOT LIKE '%trav%' AND plan_name NOT LIKE '%wea%'
QUALIFY Row_Number()Over(PARTITION BY service_id ORDER BY multi_month_cycle_end_dt DESC)=1) AUD ON t1.ESN=AUD.ESN

LEFT JOIN 
(SELECT ESN, status AS traf_status, plan_name AS Traffic, multi_month_cycle_start_dt AS Traf_St_Dt, multi_month_cycle_end_dt AS Traf_End_dt FROM t1 WHERE plan_name LIKE '%traf%' 
QUALIFY Row_Number()Over(PARTITION BY service_id ORDER BY multi_month_cycle_end_dt DESC)=1) TRAF ON t1.ESN=Traf.ESN


LEFT JOIN 
(SELECT ESN, status AS trav_status, plan_name AS Travel, multi_month_cycle_start_dt AS Trav_St_Dt, multi_month_cycle_end_dt AS Trav_End_dt  FROM t1 WHERE plan_name LIKE '%TRAV%' 
QUALIFY Row_Number()Over(PARTITION BY service_id ORDER BY multi_month_cycle_end_dt DESC)=1) TRAV ON t1.ESN=Trav.ESN

QUALIFY Row_Number()Over(PARTITION BY t1.ESN ORDER BY t1.ESN DESC)=1
) WITH DATA PRIMARY INDEX(esn);

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE;
-----------------------------------------------------------------------
----Delete records with active audio 
-----------------------------------------------------------------------
DELETE FROM dp_wedw_snd.SP_COTERM_Current_Status
WHERE Aud_status <> 3
OR aud_status IS NULL;

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE;

-----------------------------------------------------------------------
---Delete records with no data active
-----------------------------------------------------------------------
DELETE FROM dp_wedw_snd.SP_COTERM_Current_Status
WHERE traffic IS NULL 
AND  travel IS NULL;

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE;

 ----Join extract to get transaction and current status of record 
---dp_wedw_snd.SP_COTERM_Remediation - 2017-06-27
 --dp_wedw_snd.SP_COTERM_Remediation_20170707
  --dp_wedw_snd.SP_COTERM_Remediation_20170710
  
DROP TABLE dp_wedw_snd.SP_COTERM_Remediation_$CURRENT_DATE; 
 
CREATE TABLE dp_wedw_snd.SP_COTERM_Remediation_$CURRENT_DATE AS (
SELECT a.esn, aud_status,receipt_ts,Cast(event_ts AS DATE Format 'yyyy-mm-dd')event_ts,  Audio, traffic, travel, aud_end_dt, traf_status, traf_end_dt, trav_status, trav_end_dt,
				CASE  
					WHEN traffic LIKE '%Persistent%' AND travel LIKE '%Persistent%' THEN 'On Persistent Plan'
					WHEN traffic LIKE 'CVO%' OR  travel LIKE 'CVO%' THEN 'CVO - Co-Term'
				ELSE 'On CO-Term Plan'
				END AS Data_Plan_Type, vin, filename, transactionid,corpid, eventname, capability, errmsg, channel_type, event_cd, partnersalescode, source_system
FROM dp_wedw_snd.SP_COTERM_Current_Status a 
INNER JOIN dp_wedw_snd.SP_COTERM_Extract b 
ON a.esn = b.esn 
) WITH DATA PRIMARY INDEX(transactionid);

 
	
	----get current SMS CNA 
	DROP TABLE dp_wedw_snd.SP_COTERM_CNA_in_SMS;

	CREATE  TABLE dp_wedw_snd.SP_COTERM_CNA_in_SMS
AS
(
SELECT D.ESN
      ,ANA.FIRST_NAME
      ,ANA.LAST_NAME
      ,ANA.ADDRESS
      ,ANA.CITY
      ,ANA.STATE
      ,ANA.ZIP
      ,XMS.prod_name,xms.plan_type2,xms.Plan_NewSubType_Desc, receipt_ts
FROM   dp_wedw_snd.SP_COTERM_Remediation_$CURRENT_DATE       D
       INNER JOIN
       DP_VEDW_XMS.V_STATEESNDIMENSION             S
ON     D.ESN = s.ESN
       LEFT JOIN
       DP_VEDW_BIZ_XM.PLAN                         SP
ON     S.PLAN_ID = SP.PLAN_ID
       LEFT JOIN
       DP_VEDW_REP_XMS.V_REPTBL_RATEPLAN           XMS
ON     S.RATEPLAN = XMS.PROD_ID
       LEFT JOIN
       DP_VEDW_BIZ_XM.ACCT                         A
ON     S.ACCOUNTNUMBER = A.ACCT_NUM
       INNER JOIN
       DP_VEDW_BIZ_XM.ACCT_NAME_ADDRESS   ANA
ON     A.ACCT_ID = ANA.ACCT_ID
WHERE  S.CHANGEDATEEND  = '2999-12-31'
AND    ANA.ADDRESS_TYPE = 'SERVICE'
AND d.eventname LIKE  '%Sold%'
) WITH DATA PRIMARY INDEX (ESN);

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE;

---VALIDATE CNA MATCH

DROP TABLE dp_wedw_snd.SP_COTERM_CNA_Match ;

CREATE TABLE dp_wedw_snd.SP_COTERM_CNA_Match AS (
SELECT a.transactionid, a.esn, a.vin,filename, audio last_audio_plan, Data_Plan_Type, eventname,Cast(event_ts AS DATE Format 'yyyy-mm-dd')event_ts,event_cd, partnersalescode, 
				a.corpid,veh_stat_dt, a.receipt_ts,errmsg, c.firstname, c.lastname, c.address, c.city, c.state, c.zip, c.phonenumber, c.email
			,scr_modelyear VEHICLE_YEAR, scr_make vehicle_make, scr_model vehicle_model,source_system,capability,
				CASE 
					 WHEN   (Trim(Upper(b.LAST_NAME))             = Trim(Upper(LASTNAME))              -- BEGIN - 01-06-2016 
						OR   (Trim(Upper(b.FIRST_NAME))            = Trim(Upper(FIRSTNAME))             -- SCORE 295 CNA Match
						AND   Trim(Upper(Substr(b.LAST_NAME,1,2))) = Trim(Upper(Substr(LASTNAME,1,2)))) -- Criteria Change
						OR    Trim(Upper(Substr(b.ADDRESS,1,9)))   = Trim(Upper(Substr(c.ADDRESS,1,9))))  -- END
					THEN 'CNA MATCH'
					ELSE 'CNA DOES NOT MATCH'
				END AS CNA_MATCH_STATUS
	FROM dp_wedw_snd.SP_COTERM_Remediation_$CURRENT_DATE a 
	LEFT JOIN dp_wedw_snd.SP_COTERM_CNA_in_SMS b
	ON a.esn = b.esn 
	LEFT JOIN DP_VEDW_BIZ_SPO.v_StateLifDimension c
	ON a.esn = c.esn
	WHERE  changedateend >DATE 
) WITH DATA;

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE;

---Get current SMS Status 

DROP TABLE dp_wedw_snd.SP_COTERM_Bucket;

 CREATE TABLE dp_wedw_snd.SP_COTERM_Bucket AS (
SEL  b.*,rateplan_date, -- plan_desc,xps.plan_name,r
CASE 
	WHEN plan_desc LIKE '%trial:other%' AND last_audio_plan = 'CVO - Premier - 3mo' THEN 'CVO - Trial'
	WHEN plan_desc LIKE '%demo:other%' AND last_audio_plan = 'CVO - Premier - 3mo' THEN 'CVO - Demo'
	WHEN plan_desc LIKE '%trial:other%' THEN 'Trial'
	WHEN plan_desc LIKE '%self-pay%' THEN 'Self paid' 
	WHEN plan_desc LIKE '%trial:new-car%' THEN 'New Car Trial'
	WHEN plan_desc LIKE '%trial:used-car%' THEN 'Used Car Trial'
	WHEN plan_desc LIKE '%demo:used-car%' THEN 'Used Car Demo'
	WHEN plan_desc LIKE '%demo:other%' THEN 'Demo'
	WHEN plan_desc LIKE '%demo:new-car%' THEN 'New Car Demo'
	WHEN subtype = 'Selfpay' AND PlanCategory = 'Revenue' THEN 'Self paid'
	END AS bucket
--, CASE WHEN statuscode = 'A' THEN 'Active' ELSE 'Cancelled' END AS SMS_STATUS 
 FROM dp_wedw_snd.SP_COTERM_CNA_Match b
 LEFT JOIN  dp_vedw_xms.v_stateesndimension s
ON b.esn = s.esn
LEFT JOIN DP_VEDW_Rep_XMS.v_reptbl_Rateplan xp ---xm plan properties
ON s.Rateplan = xp.prod_id
LEFT JOIN DP_VEDW_BIZ_XM.plan xps
ON s.plan_id = xps.plan_id
WHERE s.changedateend >DATE
AND statuscode = 'A'
) WITH DATA;

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE;

--SEL * FROM dp_wedw_snd.SP_COTERM_SMS_Summary 
---SET ACTIONS
--1 - DBTB
--2 - DBT UCSold CNA
-- dp_wedw_snd.SP_COTERM_Rem_Final - 2017-06-27
------

DROP TABLE dp_wedw_snd.SP_COTERM_Rem_Final_$CURRENT_DATE;

CREATE TABLE dp_wedw_snd.SP_COTERM_Rem_Final_$CURRENT_DATE AS (
SELECT a.*, 
			CASE 
				WHEN Coalesce(Trim(zip), '') NOT BETWEEN '00000' AND '99999' THEN 'Bad CNA.Do nothing'
				--WHEN SMS_STATUS = 'Cancelled' THEN 'Do Nothing'
				WHEN bucket = 'Self paid' AND rateplan_date > Current_Date -30 THEN 'Do nothing.On Selfpay'
				WHEN errmsg IS NULL THEN 'Do Nothing'
				WHEN event_cd  = 'VehicleReceived'  AND bucket = 'CVO - Trial' THEN 'DBTB Demo'
				WHEN event_cd  = 'VehicleReceived' AND bucket = 'New Car Trial' THEN 'DBTB Demo'
				WHEN event_cd  = 'VehicleReceived' AND bucket = 'Self paid' THEN 'DBTB Demo'
				WHEN event_cd  = 'VehicleReceived' AND bucket = 'Used Car Demo' AND    rateplan_date < Current_Date - 180 THEN 'DBTB Demo'
				WHEN event_cd  = 'VehicleReceived' AND bucket = 'Used Car Trial' THEN 'DBTB Demo'
				---VehicleSold
				WHEN event_cd  = 'VehicleSold'  AND firstname = 'No CNA' THEN 'Do nothing. Missing CNA'
				WHEN event_cd  = 'VehicleSold'  AND bucket = 'CVO - Trial' AND CNA_Match_Status = 'CNA DOES NOT MATCH' THEN 'DBT Used Sold CNA'
				WHEN event_cd  = 'VehicleSold' AND bucket = 'New Car Trial' AND CNA_Match_Status = 'CNA DOES NOT MATCH' THEN 'DBT Used Sold CNA'
				WHEN event_cd  = 'VehicleSold' AND bucket = 'Self paid' AND CNA_Match_Status = 'CNA DOES NOT MATCH' THEN 'DBT Used Sold CNA'
				WHEN event_cd  = 'VehicleSold' AND bucket = 'Used Car Demo' THEN 'DBT Used Sold CNA'
				WHEN event_cd  = 'VehicleSold' AND bucket = 'Used Car Trial' AND CNA_Match_Status = 'CNA DOES NOT MATCH' THEN 'DBT Used Sold CNA'
			ELSE 
				'Do Nothing'
			END  AS Remediation_Action
FROM dp_wedw_snd.SP_COTERM_Bucket a
)WITH DATA;										

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE;


.set echoreq off;
.set titledashes off;
.SET WIDTH 720
.Set Format OFF
.EXPORT DATA FILE= '/appinfprd/bi/infogix/IA83/InfogixClient/scripts_edm/Dev/Remidationscripts/DRTfiles/Non_coterm/$FILE_NM'
.SET RECORDMODE OFF


------------------------------------------------------------
-- SOLD
-- DEACTIVATE BRAND TRIGGER
------------------------------------------------------------

SELECT
DISTINCT esn || ',' ||
  				Trim(Cast(b.PRIMARY_BRAND_ID AS INT)) || ',' || -- primaryCorpID
				Trim(Cast(b.SECONDARY_BRAND_ID AS INT))   || ',' || -- secondaryCorpID
				CASE WHEN  filename LIKE 'ford_cpo%' OR corpid = 8457 THEN  'UCFORDAA' ELSE  'UCTRIALAA' END || ',' ||
'' || ',' || -- primaryCorpID2
'' || ',' || -- secondaryCorpID2
'' || ',' || -- promoCode2
'' || ',' || -- esn2
'' || ',' || -- accountID
'' || ',' || -- planID
Cast(Cast(Current_Date  AS Format 'YYYY-MM-DD') AS CHAR(10)) || ',' ||
CASE WHEN FIRSTNAME IS NULL THEN '-'
                                                WHEN FIRSTNAME LIKE ('%,%') THEN Substr(FIRSTNAME,1,Position(',' IN FIRSTNAME)-1) || Substr(FIRSTNAME, Position(',' IN FIRSTNAME)+1, Character_Length(FIRSTNAME)) ELSE FIRSTNAME END || ',' ||
CASE WHEN LASTNAME IS NULL THEN '-'
                                                WHEN LASTNAME LIKE ('%,%') THEN Substr(LASTNAME,1,Position(',' IN LASTNAME)-1) || Substr(LASTNAME, Position(',' IN LASTNAME)+1, Character_Length(LASTNAME)) ELSE LASTNAME END || ',' ||
CASE WHEN ADDRESS IS NULL THEN ''
                                                WHEN ADDRESS LIKE ('%,%') THEN RegExp_Replace ( ADDRESS, ',', ' '  ) ELSE ADDRESS END || ',' ||
CASE WHEN CITY IS NULL THEN ''
                                                WHEN CITY LIKE ('%,%') THEN Substr(CITY,1,Position(',' IN CITY)-1) || Substr(CITY, Position(',' IN CITY)+1, Character_Length(CITY)) ELSE CITY END || ',' ||

       Upper(CASE WHEN STATE IS NULL 
            THEN ''
            WHEN STATE LIKE ('%,%') 
            THEN Substr(STATE,1,Position(',' IN STATE)-1) || Substr(STATE, Position(',' IN STATE)+1, Character_Length(STATE))
            ELSE STATE
       END)                                           || ',' || -- 16. State - Added UPPER conversion 6-9-2016											
CASE
      WHEN Coalesce(Trim(ZIP), '') BETWEEN '0' AND '99999' THEN ZIP
      ELSE 'Canadian zip'
   END ||','||
--COALESCE(TRIM(ZIP), '') || ',' ||
Coalesce(Trim(PHONENUMBER), 'nophone') || ',' ||
CASE WHEN EMAIL = 'NoCNA@NoCNA.usedcar' THEN 'noemail'
                WHEN Trim(EMAIL) = '' THEN 'noemail'
                ELSE Coalesce(Trim(EMAIL), 'noemail') END || ',' ||
	   VIN                                            || ',' || -- VIN
      Trim(a.vehicle_year)     || ',' || -- Vehicle Year
       Trim(a.vehicle_make)                         || ',' || -- Vehicle Make 
      Trim(a.Vehicle_model)                            || ',' || -- Vehicle Model 
       ''                                             || ',' || -- Model Num
      Trim(Cast(transactionid AS INT))-- SCORE Transaction Id
      || ',' || ',' || eventname ||','|| Cast(event_ts AS DATE Format 'yyyy-mm-dd') ||','|| capability ||','|| source_system
	   AS DEACT_BRAND_TRIGGER		 
 FROM dp_wedw_snd.SP_COTERM_Rem_Final_$CURRENT_DATE  a
 LEFT JOIN DP_VEDW_BIZ_XM.RADIO_RECEIVER b
ON a.esn = b.accs_meth_id
WHERE Remediation_Action = 'DBT Used Sold CNA'
AND (
                FIRSTNAME IS NOT NULL AND Trim(FIRSTNAME) NE '' OR
                LASTNAME IS NOT NULL AND Trim(LASTNAME) NE ''
)
AND ADDRESS IS NOT NULL AND Trim(ADDRESS) NE ''
AND CITY IS NOT NULL AND Trim(CITY) NE ''
AND STATE IS NOT NULL AND Trim(STATE) NE ''
AND ZIP IS NOT NULL AND Trim(ZIP) NE ''
ORDER BY firstname;
					
.EXPORT RESET



.set echoreq off;
.set titledashes off;
.SET WIDTH 720
.Set Format OFF
.EXPORT DATA FILE= '/appinfprd/bi/infogix/IA83/InfogixClient/scripts_edm/Dev/Remidationscripts/DRTfiles/Non_coterm/$FILE_NM1'
.SET RECORDMODE OFF

----------------------------------------------------------------------------------------------------------
-- INVENTORY
-- DEACTIVATE BRAND TRIGGER REBRAND
----------------------------------------------------------------------------------------------------------
SELECT
DISTINCT esn || ',' ||
  			Trim(Cast(b.PRIMARY_BRAND_ID AS INT)) || ',' || -- primaryCorpID
			Trim(Cast(b.SECONDARY_BRAND_ID AS INT))   || ',' || -- secondaryCorpID
			'UCOTLDEMOPREM'	 || ',' ||
'' || ',' || -- primaryCorpID2
'' || ',' || -- secondaryCorpID2
CASE WHEN  filename LIKE 'ford_cpo%' OR corpid = 8457 THEN  'UCFORDAA' ELSE  'UCTRIALAA' END || ',' || -- promoCode2
'' || ',' || -- esn2
'' || ',' || -- accountID
'' || ',' || -- planID
Cast(Cast(Current_Date  AS Format 'YYYY-MM-DD') AS CHAR(10)) || ',' ||
'' || ',' || -- FIRSTNAME
'' || ',' || -- LASTNAME
'' || ',' || -- ADDRESS
'' || ',' || -- CITY
'' || ',' || -- STATE
'' || ',' || -- ZIP
'' || ',' || -- PHONENUMBER
'' || ',' || -- EMAIL 
       VIN                                            || ',' || -- VIN
       Trim(a.VEHICLE_YEAR)      || ',' || -- Vehicle Year
       Trim(a.Vehicle_Make)                             || ',' || -- Vehicle Make 
       Trim(a.Vehicle_Model)                            || ',' || -- Vehicle Model 
       ''                                             || ',' || -- Model Num
       Trim(Cast(transactionid AS INT))-- SCORE Transaction Id
      || ',' || ',' || eventname ||','|| Cast(event_ts AS DATE Format 'yyyy-mm-dd') ||','|| capability ||','|| source_system AS DEACT_BRAND_TRIGGER_BRAND
FROM  dp_wedw_snd.SP_COTERM_Rem_Final_$CURRENT_DATE  a
LEFT JOIN DP_VEDW_BIZ_XM.RADIO_RECEIVER b
ON a.esn = b.accs_meth_id
WHERE Remediation_Action = 'DBTB Demo';

.EXPORT RESET

EOF

bteq<<EOF
.logoff
.quit
exit 0



