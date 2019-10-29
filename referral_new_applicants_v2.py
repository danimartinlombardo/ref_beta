import psycopg2
import pymysql
import os
import sys
import requests
import time
from credentials import *
import re

start_time = time.time()
duplicated = 0
new = 0

braze_headers = slack_headers = amplitude_headers = {'Content-Type': "application/json", 'Cache-Control': "no-cache"}

#DEF METHOD TO SEND SLACK MESSAGES TO #GRW-ALERTS
def slack_message (text):
	slack_payload = "{\n\t\"text\": \""+os.path.basename(__file__)+text+"\"\n}"
	slack = requests.request("POST", slack_url, data=slack_payload, headers=slack_headers)

#DEF METHOD TO GET NEW PROGRAM PARTICIPANTS FOR EACH REGION FROM REDSHIFT
def new_participants (region_id, required_do_num, week_num_limit, amount_granted_godfather, amount_granted_applicant, currency, currency_factor, tax_code):
	try:
		con_rs=psycopg2.connect(dbname= 'dwh', host='cabify-datawarehouse.cxdpjwjwbg9i.eu-west-1.redshift.amazonaws.com', port= '5439', user= db_rs_user, password= db_rs_pass)
		cur_rs= con_rs.cursor()
		cur_rs.execute('''
			SELECT
				applicant.id_driver as applicant_id, /* aplicant[0] */
				lower(trim(applicant2.ds_email)) as applicant_email, /* aplicant[1] */
				applicant.ds_name||' '||applicant.ds_surname as applicant_fullname, /* aplicant[2] */
				j.id_journey as first_do_journey_id, /* aplicant[3] */
				min_do.tm_start_local_at as first_do_local_dttm, /* aplicant[4] */
				DATEADD(day, 1, (DATEADD(week, %s, min_do.tm_start_local_at))) as dateline_dttm, /* aplicant[5] */
				r.id_region as first_do_region_id, /* aplicant[6] */
				r.ds_time_zone as time_zone, /* aplicant[7] */
				a.id_agency as first_do_agency_id, /* aplicant[8] */
				lower(trim(ad.ds_driver_invitation_code)) as applicant_code, /* aplicant[9] */
				godfather.id_user as godfather_id, /* aplicant[10] */
				0 as do_num, /* aplicant[11] */
				'on_time' as state, /* aplicant[12] */
				%s as conditions_week_num, /* aplicant[13] */
				%s as conditions_required_do, /* aplicant[14] */
				%s as conditions_amount_granted_godfather, /* aplicant[15] */
				%s as conditions_amount_granted_applicant, /* aplicant[16] */
				NULL as bonus_request_id, /* aplicant[17] */
				getdate() as created_at_utc, /* aplicant[18] */
				getdate() as updated_at_utc, /* aplicant[19] */
				null as updated_at_local, /* aplicant[20] */
				0 as do_strange_num, /* aplicant[21] */
				%s as currency, /* aplicant[22] */
				%s as currency_factor, /* aplicant[23] */
				%s as tax_code, /* aplicant[24] */
				godfather.ds_name||' '||godfather.ds_surname as godfather_fullname, /* aplicant[25] */
				applicant.id_driver||godfather.id_user as combo /* NOT INSERTED */
			FROM
				datawarehouse.ops_fac_journey_min_do_driver min_do
				inner join datawarehouse.ops_fac_journey j on min_do.sk_journey = j.sk_journey
				inner join datawarehouse.ops_dim_agency a on j.fk_agency_id = a.sk_agency
				inner join datawarehouse.ops_dim_region r on j.fk_region_id = r.sk_region
				inner join datawarehouse.ops_dim_driver applicant on min_do.fk_driver_id = applicant.sk_driver
				inner join datawarehouse.ops_dim_user applicant2 on applicant.id_driver = applicant2.id_user
				inner join datawarehouse.lgt_fac_applicantdetail ad on applicant.fk_applicant_id = ad.sk_applicantdetail
				inner join datawarehouse.ops_dim_user godfather on lower(trim(ad.ds_driver_invitation_code)) = lower(trim(godfather.ds_email))
			WHERE
				j.dt_start_local_at > date_trunc('day', DATEADD(day, -5, GETDATE()))
				and j.dt_start_local_at < date_trunc('day', GETDATE())
				and r.id_region IN (%s)
	            and lower(trim(ad.ds_driver_invitation_code)) != lower(trim(applicant2.ds_email)) /*TO AVOID AUTOREFERRALS*/
	            and godfather.fk_company_id != -1 /*TO AVOID REFERRED BY USERS NOT DRIVERS*/
	        ''',(week_num_limit, week_num_limit, required_do_num,amount_granted_godfather,amount_granted_applicant, currency, currency_factor, tax_code, region_id))
		global valid_applicants
		valid_applicants = cur_rs.fetchall()
		print (len(valid_applicants))
	except psycopg2.Error as e:
		slack_message(': <!channel> ERROR Unable to read new participants for region '+region_id+': '+ str(e))
		exit()

os.system('clear')

###LOAD CURRENT REGION CONFIGURATION
print ('Fetching current region configurations... ', end='')
try:
	con_ms = pymysql.connect(db= 'GRW_drivers', host='35.195.80.162', user=db_ms_user, password= db_ms_pass)
	cur_ms = con_ms.cursor()
	cur_ms.execute("""
		SELECT t1.*
		FROM referral_region_config t1
		WHERE t1.created_at =
			(SELECT t2.created_at
			FROM referral_region_config t2
			WHERE t2.region_id = t1.region_id            
			ORDER BY t2.created_at DESC
			LIMIT 1)
		""")
except Exception as e:
	slack_message(': <!channel> ERROR Unable to read current region configurations: '+ str(e))
	exit()
region_config = cur_ms.fetchall()
print (len(region_config))

###FETCH ALL TIME PARTICIPANTS
print ('Fetching all time participants... ', end='')
try:
	con_ms = pymysql.connect(db= 'GRW_drivers', host='35.195.80.162', user=db_ms_user, password= db_ms_pass)
	cur_ms = con_ms.cursor()
	cur_ms.execute('''
		SELECT
			distinct (applicant_id)
		FROM
			referral_participants
		''')
except Exception as e:
	slack_message(': <!channel> ERROR Unable to read all time participants: '+ str(e))
	exit()
current_applicants = cur_ms.fetchall()
print (len(current_applicants))
current_applicants_id=[i[0] for i in current_applicants]

###FETCH CURRENT ACTIVE APPLICANTS
print ('Fetching active participants... ', end='')
try:
	con_ms = pymysql.connect(db= 'GRW_drivers', host='35.195.80.162', user=db_ms_user, password= db_ms_pass)
	cur_ms = con_ms.cursor()
	cur_ms.execute('''
		SELECT
			distinct (applicant_id)
		FROM
			referral_participants
		WHERE
			state != 'obsolete'
		''')
except Exception as e:
	slack_message(': <!channel> ERROR Unable to read active participants: '+ str(e))
	exit()
active_applicants = cur_ms.fetchall()
print (len(active_applicants))
active_applicants_id=[i[0] for i in active_applicants]

###ADD NEW APPLICANTS
for region in region_config:
	print ('Fetching new valid applicants for region '+region[0]+': ', end='')
	region_new = 0
	region_duplicated= 0
	try:
		new_participants(region[0],region[1],region[2],region[3],region[4],region[5],region[6],region[7])
	except:
		slack_message(': <!channel> ERROR Unable to read new participants for region '+region_id)
		print('ERROR Unable to read new participants for region '+region_id)
		continue
	for applicant in valid_applicants:
		if applicant[0] in current_applicants_id:
			print (applicant[0] + ' Skipped: already on the program')
			duplicated = duplicated + 1
			region_duplicated = region_duplicated + 1
			continue
		try:
			cur_ms.execute('''
				INSERT INTO referral_participants
				VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
				(applicant[0], applicant[1], applicant[2], applicant[3], applicant[4], applicant[5], applicant[6], applicant[7], applicant[8], applicant[9], applicant[10], applicant[11], applicant[12], applicant[13], applicant[14], applicant[15], applicant[16], applicant[17], applicant[18], applicant[19], applicant[20], applicant[21], applicant[22], applicant[23], applicant[24], applicant[25]))
			con_ms.commit()
			new = new + 1
			region_new = region_new + 1
			print (applicant[0] + ' applicant included', end='')
			try:
				braze_payload = "{\n  \"api_key\": \""+braze_api+"\",\n  \"campaign_id\": \"3b3e9cbd-f984-b2ad-89a0-4c8a3e3a90a4\",\n  \"recipients\": [\n     {\n      \"external_user_id\": \""+applicant[10]+"\"\n     }\n   ]\n}"
				response = requests.request("POST", url = "https://rest.iad-01.braze.com/campaigns/trigger/send", data=braze_payload, headers=braze_headers)
				print ('. Braze response:'+response.text)
			except:
				print(': <!channel> ERROR Unable to send push to godfather (new applicants)')
		except pymysql.Error as e:
			slack_message(': <!channel> ERROR Unable to insert new participants: '+ str(e))
			exit()
	slack_message(":\nRegion {0} data.\nNew applicants: {1}\nExcluded duplicated: {2}".format(region[0], region_new, region_duplicated))

###UPDATE COHORT IN AMPLITUDE
print ('Updating cohort in Amplitude')
try:
	con_ms = pymysql.connect(db= 'GRW_drivers', host='35.195.80.162', user=db_ms_user, password= db_ms_pass)
	cur_ms = con_ms.cursor()
	cur_ms.execute('''
		SELECT distinct(applicant_id) from referral_participants
		''')
	applicants_raw = cur_ms.fetchall()
	applicants_id=[i[0] for i in applicants_raw]
	applicants_id_string = '''\"'''
	for y in range(len(applicants_id)-1):
		applicants_id_string = applicants_id_string+applicants_id[y]+'''\",\n\"'''
	applicants_id_string = applicants_id_string+applicants_id[len(applicants_id)-1]+'''\"'''
except Exception as e:
	slack_message(': <!channel> ERROR Unable to prepare applicants string: '+ str(e))
amplitude_payload = "{\"name\":\"GRW_referrals_participants\",\"app_id\":174786,\"id_type\":\"BY_USER_ID\",\"ids\":[\n"+applicants_id_string+"],\"owner\":\"daniel.martin@cabify.com\",\"published\":true,\"existing_cohort_id\":\"jlqin13\"}"
response = requests.request("POST", url = 'https://amplitude.com/api/3/cohorts/upload', headers=amplitude_headers, data=amplitude_payload, auth=(amplitude_apikey, amplitude_secretkey))
print ('Amplitude response: '+response.text)
slack_message(": Script loaded succesfully. Runtime: {0} seconds.\nExisting participants: {1}\nNew applicants: {2}\nExcluded duplicated: {3}".format((round(time.time() - start_time, 2)), len(active_applicants), new, duplicated))
