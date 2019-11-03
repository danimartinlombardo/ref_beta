import pymysql
import psycopg2
import os, sys
import requests
import time
import pandas as pd
from sqlalchemy import create_engine
from credentials import *

start_time = time.time()

braze_headers = slack_headers = {'Content-Type': "application/json", 'Cache-Control': "no-cache"}

def slack_message (text):
	slack_payload = "{\n\t\"text\": \""+os.path.basename(__file__)+text+"\"\n}"
	slack = requests.request("POST", slack_url, data=slack_payload, headers=slack_headers)	

os.system('clear')

###REFRESH REGIONS TABLE AT MYSQL
try:
	con_ms = pymysql.connect(db= 'GRW_drivers', host='35.195.80.162', user=db_ms_user, password= db_ms_pass)
	cur_ms = con_ms.cursor()
	cur_ms.execute("""DROP TABLE IF EXISTS regions""")
	con_ms.commit()
except psycopg2.Error as e:
	print('Error deleting regions table: '+ str(e))

try:
	con_rs=psycopg2.connect(dbname= 'dwh', host='cabify-datawarehouse.cxdpjwjwbg9i.eu-west-1.redshift.amazonaws.com', port= '5439', user= db_rs_user, password= db_rs_pass)
	regions = pd.read_sql_query("SELECT id_region as region_id, ds_time_zone as time_zone FROM datawarehouse.ops_dim_region", con_rs)
except psycopg2.Error as e:
	print('Error reading RS regions table: '+ str(e))

sqlEngine = create_engine('mysql+pymysql://'+db_ms_user+':'+db_ms_pass+'@35.195.80.162/GRW_drivers')
dbConnection = sqlEngine.connect()

df_regions = pd.DataFrame(data=regions)
try:
    frame = df_regions.to_sql("regions", dbConnection)
except ValueError as vx:
    print(vx)
except Exception as ex:
    print(ex)
else:
    print("Table regions created successfully.");
finally:
    dbConnection.close()

#REFRESH JOURNEYS TABLE AT MYSQL
try:
	con_ms = pymysql.connect(db= 'GRW_drivers', host='35.195.80.162', user=db_ms_user, password= db_ms_pass)
	cur_ms = con_ms.cursor()
	cur_ms.execute("""DROP TABLE IF EXISTS journeys""")
	con_ms.commit()
except psycopg2.Error as e:
	print('Error deleting journeys table: '+ str(e))

try:
	con_ms = pymysql.connect(db= 'GRW_drivers', host='35.195.80.162', user=db_ms_user, password= db_ms_pass)
	active_participants = pd.read_sql_query("""SELECT applicant_id, dateline_dttm FROM referral_participants WHERE state = 'on_time'""", con_ms)
	applicant_list = active_participants.values.tolist()
	print("Reading active participants succeeded: "+str(len(applicant_list))+" participants")
except pymysql.Error as e:
	print('Error reading active participants: '+ str(e))

try:
	con_rs=psycopg2.connect(dbname= 'dwh', host='cabify-datawarehouse.cxdpjwjwbg9i.eu-west-1.redshift.amazonaws.com', port= '5439', user= db_rs_user, password= db_rs_pass)
	journeys = pd.read_sql_query('''
		SELECT
			j.id_journey as journey_id,
			d.id_driver as driver_id,
			es.ds_end_state as end_state,
			j.tm_start_utc_at as start_at,
			r.id_region as region_id,
			jt.ds_pricing_source
		FROM datawarehouse.ops_fac_journey j
			inner join datawarehouse.ops_dim_end_state es ON j.fk_end_state_id = es.sk_end_state
			inner join datawarehouse.ops_dim_driver d ON j.fk_driver_id = d.sk_driver
			inner join datawarehouse.ops_fac_journeytotals jt ON jt.fk_journey_id = j.sk_journey
			inner join datawarehouse.ops_dim_region r on j.fk_region_id = r.sk_region
		WHERE
			j.id_journey = 'd037ea5c71eb45b28fbfccc4ef9db08b'
		''',con_rs)
	print("Initial dataframe created")
except psycopg2.Error as e:
	print('Error creating initial dataframe: '+ str(e))

applicant_counter = 0
for applicant in applicant_list:
	applicant_counter = applicant_counter + 1
	try:
		con_rs=psycopg2.connect(dbname= 'dwh', host='cabify-datawarehouse.cxdpjwjwbg9i.eu-west-1.redshift.amazonaws.com', port= '5439', user= db_rs_user, password= db_rs_pass)
		journeys_driver = pd.read_sql_query('''
			SELECT
				j.id_journey as journey_id,
				d.id_driver as driver_id,
				es.ds_end_state as end_state,
				j.tm_start_utc_at as start_at,
				r.id_region as region_id,
				jt.ds_pricing_source
			FROM datawarehouse.ops_fac_journey j
				inner join datawarehouse.ops_dim_end_state es ON j.fk_end_state_id = es.sk_end_state
				inner join datawarehouse.ops_dim_driver d ON j.fk_driver_id = d.sk_driver
				inner join datawarehouse.ops_fac_journeytotals jt ON jt.fk_journey_id = j.sk_journey
				inner join datawarehouse.ops_dim_region r on j.fk_region_id = r.sk_region
			WHERE
				d.id_driver = %(id)s
				and j.tm_start_utc_at < date_trunc('day', DATEADD(day, +1, %(te)s))
				and j.fk_end_state_id = 4
			''',con_rs, params={"id":applicant[0], "te":applicant[1]})
		journeys = journeys.append(journeys_driver, ignore_index=True)
		if applicant_counter == len(applicant_list):
			print("Inserting participant "+str(applicant_counter))
		else: print("Inserting participant "+str(applicant_counter), end="\r")
	except psycopg2.Error as e:
		print('Error reading RS journeys table: '+ str(e))
print("Dataframe shape: "+str(journeys.shape))

sqlEngine = create_engine('mysql+pymysql://'+db_ms_user+':'+db_ms_pass+'@35.195.80.162/GRW_drivers')
dbConnection = sqlEngine.connect()

try:
    frame = journeys.to_sql("journeys", dbConnection)
except ValueError as vx:
    print(vx)
except Exception as ex:
    print(ex)
else:
    print("Table journeys created successfully.");
finally:
    dbConnection.close()

###UPDATE APPLICANTS: DB
try:
	con_ms = pymysql.connect(db= 'GRW_drivers', host='35.195.80.162', user=db_ms_user, password= db_ms_pass)
	cur_ms = con_ms.cursor()
	cur_ms.execute("""
	UPDATE referral_participants rfp
		inner join (
		SELECT
				a.applicant_id,
				a.do_num,
				a.dateline_dttm,
				a.time_zone,
				a.conditions_required_do,
				a.do_strange_num
			FROM (
				SELECT
					rf.applicant_id,
					rf.dateline_dttm,
					rf.conditions_required_do,
					r.time_zone,
					count(j.journey_id) as do_num,
					SUM(case when j.ds_pricing_source = 'strange_journey' then 1 else 0 end) as do_strange_num
				FROM
					referral_participants rf
					inner join journeys j on rf.applicant_id = j.driver_id
					inner join regions r on j.region_id = r.region_id
				WHERE
					CONVERT_TZ(j.start_at, 'UTC', r.time_zone) < rf.dateline_dttm
					and j.end_state = 'drop off'
					and rf.state != 'obsolete'
				group by 1,2,3,4) a
      		) AS data_update
		SET
			rfp.do_num = (data_update.do_num - data_update.do_strange_num),
			rfp.do_strange_num = data_update.do_strange_num,
    		rfp.state = (case
					when data_update.dateline_dttm < DATE_ADD(CONVERT_TZ(Now(), 'UTC', data_update.time_zone), Interval -9 DAY ) then 'obsolete'
					when data_update.dateline_dttm < DATE_ADD(CONVERT_TZ(Now(), 'UTC', data_update.time_zone), Interval -7 DAY ) then 'clear'
					when (data_update.do_num - data_update.do_strange_num)  >= data_update.conditions_required_do then 'achieved'
					when data_update.dateline_dttm > (CONVERT_TZ(Now(), 'UTC', data_update.time_zone)) then 'on_time'
					else 'expired'
				end),
    		rfp.updated_at_utc = Now(),
    		rfp.updated_at_local = CONVERT_TZ(Now(), 'UTC', data_update.time_zone)
		WHERE
			rfp.applicant_id=data_update.applicant_id;
		""")
	con_ms.commit()
except psycopg2.Error as e:
	slack_message(': <!channel> ERROR Unable to update participants data: '+ str(e))
	print(': <!channel> ERROR Unable to update participants data: '+ str(e))
	exit()
#print('Program DO & states updated')

###UPDATE APPLICANTS: BRAZE (GODFATHERS)
try:
	cur_ms.execute("""	
		SELECT
			q.external_id,
			GROUP_CONCAT(q.fullname_quote SEPARATOR ', ') as referrals_name,
			GROUP_CONCAT(q.email_quote SEPARATOR ', ') as referrals_email,
			GROUP_CONCAT(q.dateline_quote SEPARATOR ', ') as referrals_dateline,
			GROUP_CONCAT(q.required_do_quote SEPARATOR ', ') as referrals_required_do,
			GROUP_CONCAT(q.state_quote SEPARATOR ', ') as referrals_state,
			GROUP_CONCAT(q.actual_do_quote SEPARATOR ', ') as referrals_actual_do,
			GROUP_CONCAT(q.updated_at_local_quote SEPARATOR ', ') as referrals_updated_at_local,
			GROUP_CONCAT(q.week_num_quote SEPARATOR ', ') as referrals_conditions_week_num,
			GROUP_CONCAT(q.amount_godfather_quote SEPARATOR ', ') as referrals_conditions_godfather_amount,
			GROUP_CONCAT(q.amount_applicant_quote SEPARATOR ', ') as referrals_conditions_applicant_amount
		FROM
			(SELECT
				rp.godfather_id as external_id,
				(case when rp.state='clear' then '' else rp.applicant_fullname end ) as fullname_quote,
				(case when rp.state='clear' then '' else rp.applicant_email end) as email_quote,
				(case when rp.state='clear' then '' else (CAST(rp.dateline_dttm as char)) end) as dateline_quote,
				(case when rp.state='clear' then '' else (CAST(rp.conditions_required_do as char)) end) as required_do_quote,
				(case when rp.state='clear' then '' else rp.state end) as state_quote,
				(case when rp.state='clear' then '' else (CAST(rp.do_num as char)) end) as actual_do_quote,
				(case when rp.state='clear' then '' else (CAST(rp.updated_at_local as char)) end) as updated_at_local_quote,
				(case when rp.state='clear' then '' else (CAST(rp.conditions_week_num as char)) end) as week_num_quote,
				(case when rp.state='clear' then '' else (CAST(rp.conditions_amount_granted_godfather as char)) end) as amount_godfather_quote,
				(case when rp.state='clear' then '' else (CAST(rp.conditions_amount_granted_applicant as char)) end) as amount_applicant_quote
			FROM
				referral_participants rp
			WHERE rp.state != 'obsolete'
			ORDER BY rp.godfather_id, rp.created_at_utc, rp.applicant_email) q
		GROUP BY 1;
	""")
	#print ('Braze arrays ready to upload')
except psycopg2.Error as e:
	slack_message(': <!channel> ERROR Unable to create Braze arrays for godfathers: '+ str(e))
	print(': <!channel> ERROR Unable to create Braze arrays for godfathers: '+ str(e))
	exit()
braze_arrays = cur_ms.fetchall()
for godfather in braze_arrays:	
	try:
		# payload as string
		braze_payload = "{\n  \"api_key\": \""+braze_api+"\",\n  \"attributes\": [ \n \t{\n \t  \"external_id\":\""+godfather[0]+"\",\n      \"referrals_name_str\": "+godfather[1]+",\n      \"referrals_email_str\": "+godfather[2]+",\n      \"referrals_dateline_str\": "+godfather[3]+",\n      \"referrals_required_do_str\": "+godfather[4]+",\n      \"referrals_state_str\": "+godfather[5]+",\n      \"referrals_actual_do_str\": "+godfather[6]+",\n      \"referrals_updated_at_local_str\": "+godfather[7]+",\n      \"referrals_conditions_week_num_str\": "+godfather[8]+",\n      \"referrals_conditions_godfather_amount_str\": "+godfather[9]+",\n      \"referrals_conditions_applicant_amount_str\": "+godfather[10]+"\n    }\n   ]\n}"
		response = requests.request("POST", url = "https://rest.iad-01.braze.com/users/track", data=braze_payload, headers=braze_headers)
		print (godfather[0] + ' Braze attributes updated. Response '+response.text)
	except:
		slack_message(': ERROR Braze attributes update error on GODFATHER_id '+godfather[0])
		print('ERROR Braze attributes update error on GODFATHER_id '+godfather[0])

###UPDATE APPLICANTS: BRAZE (APPLICANTS)
try:
	cur_ms.execute("""	
		SELECT
			q.external_id,
			GROUP_CONCAT(q.fullname_quote SEPARATOR ', ') as referrals_godfather_name,
			GROUP_CONCAT(q.email_quote SEPARATOR ', ') as referrals_godfather_email,
			GROUP_CONCAT(q.dateline_quote SEPARATOR ', ') as referrals_applicant_dateline,
			GROUP_CONCAT(q.required_do_quote SEPARATOR ', ') as referrals_applicant_required_do,
			GROUP_CONCAT(q.state_quote SEPARATOR ', ') as referrals_applicant_state,
			GROUP_CONCAT(q.actual_do_quote SEPARATOR ', ') as referrals_applicant_actual_do,
			GROUP_CONCAT(q.updated_at_local_quote SEPARATOR ', ') as referrals_applicant_updated_at_local,
			GROUP_CONCAT(q.week_num_quote SEPARATOR ', ') as referrals_applicant_week_num,
			GROUP_CONCAT(q.amount_godfather_quote SEPARATOR ', ') as referrals_applicant_amount_godfather,
			GROUP_CONCAT(q.amount_applicant_quote SEPARATOR ', ') as referrals_applicant_amount
		FROM
			(SELECT
				rp.applicant_id as external_id,
				(case when rp.state='clear' then '' else rp.godfather_fullname end ) as fullname_quote,
				(case when rp.state='clear' then '' else rp.applicant_code end) as email_quote,
				(case when rp.state='clear' then '' else (CAST(rp.dateline_dttm as char)) end) as dateline_quote,
				(case when rp.state='clear' then '' else (CAST(rp.conditions_required_do as char)) end) as required_do_quote,
				(case when rp.state='clear' then '' else rp.state end) as state_quote,
				(case when rp.state='clear' then '' else (CAST(rp.do_num as char)) end) as actual_do_quote,
				(case when rp.state='clear' then '' else (CAST(rp.updated_at_local as char)) end) as updated_at_local_quote,
				(case when rp.state='clear' then '' else (CAST(rp.conditions_week_num as char)) end) as week_num_quote,
				(case when rp.state='clear' then '' else (CAST(rp.conditions_amount_granted_godfather as char)) end) as amount_godfather_quote,
				(case when rp.state='clear' then '' else (CAST(rp.conditions_amount_granted_applicant as char)) end) as amount_applicant_quote
			FROM
				referral_participants rp
			WHERE rp.state != 'obsolete'
			ORDER BY rp.applicant_id, rp.created_at_utc, rp.applicant_email) q
		GROUP BY 1;
	""")
except psycopg2.Error as e:
	slack_message(': <!channel> ERROR Unable to create Braze arrays for applicants: '+ str(e))
	print(': <!channel> ERROR Unable to create Braze arrays for applicants: '+ str(e))
	exit()	
braze_applicants = cur_ms.fetchall()
for applicant in braze_applicants:	
	try:
		braze_payload = "{\n  \"api_key\": \""+braze_api+"\",\n  \"attributes\": [ \n \t{\n \t  \"external_id\":\""+applicant[0]+"\",\n      \"referrals_gf_name_str\": "+applicant[1]+",\n      \"referrals_gf_email_str\": "+applicant[2]+",\n      \"referrals_gf_dateline_str\": "+applicant[3]+",\n      \"referrals_gf_required_do_str\": "+applicant[4]+",\n      \"referrals_gf_state_str\": "+applicant[5]+",\n      \"referrals_gf_actual_do_str\": "+applicant[6]+",\n      \"referrals_gf_updated_at_local_str\": "+applicant[7]+",\n      \"referrals_gf_conditions_week_num_str\": "+applicant[8]+",\n      \"referrals_gf_conditions_godfather_amount_str\": "+applicant[9]+",\n      \"referrals_gf_conditions_applicant_amount_str\": "+applicant[10]+"\n    }\n   ]\n}"
		response = requests.request("POST", url = "https://rest.iad-01.braze.com/users/track", data=braze_payload, headers=braze_headers)
		print (applicant[0] + ' Braze attributes updated. Response '+response.text)
	except:
		slack_message(': ERROR Braze attributes update error on applicant_id '+applicant[0])
		print('ERROR Braze attributes update error on applicant_id '+applicant[0])
slack_message(": Script loaded succesfully. Runtime: %s seconds" % round(time.time() - start_time, 2))
