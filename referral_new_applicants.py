import psycopg2
import os
import sys
import requests
import time
from credentials import *

start_time = time.time()
duplicated = 0
new = 0

braze_headers = slack_headers = {'Content-Type': "application/json", 'Cache-Control': "no-cache"}

def slack_message (text):
	slack_payload = "{\n\t\"text\": \""+os.path.basename(__file__)+text+"\"\n}"
	slack = requests.request("POST", slack_url, data=slack_payload, headers=slack_headers)

def new_participants (agency_id, required_do_num, week_num_limit, amount_granted_godfather, amount_granted_applicant, currency, currency_factor, tax_code):
	try:
		con_rs=psycopg2.connect(dbname= 'dwh', host='cabify-datawarehouse.cxdpjwjwbg9i.eu-west-1.redshift.amazonaws.com', port= '5439', user= rs_user, password= rs_pass)
		cur_rs= con_rs.cursor()
		cur_rs.execute('''
			SELECT
				applicant.id_driver as applicant_id, /* aplicant[0] */
				lower(trim(applicant2.ds_email)) as applicant_email, /* aplicant[1] */
				applicant.ds_name||' '||applicant.ds_surname as applicant_fullname, /* aplicant[2] */
				j.id_journey as first_do_journey_id, /* aplicant[3] */
				min_do.tm_start_local_at as first_do_local_dttm, /* aplicant[4] */
				DATEADD(week, %s, min_do.tm_start_local_at) as dateline_dttm, /* aplicant[5] */
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
				j.dt_start_local_at > date_trunc('day', DATEADD(day, -4, GETDATE()))
				and j.dt_start_local_at < date_trunc('day', GETDATE())
				and a.id_agency IN (%s)
	            and lower(trim(ad.ds_driver_invitation_code)) != lower(trim(applicant2.ds_email)) /*TO AVOID AUTOREFERRALS*/
	            and godfather.fk_company_id != -1 /*TO AVOID REFERRED BY USERS NOT DRIVERS*/
	        ''',(week_num_limit, week_num_limit, required_do_num,amount_granted_godfather,amount_granted_applicant, currency, currency_factor, tax_code, agency_id))
		global valid_applicants
		valid_applicants = cur_rs.fetchall()
		print (len(valid_applicants))
	except psycopg2.Error as e:
		slack_message(': <!channel> ERROR Unable to read new participants for agency '+agency_id+': '+ str(e))
		exit()

os.system('clear')

###LOAD CURRENT AGENCY CONFIGURATION
print ('Fetching current agency configurations... ', end='')
try:
	con_pg = psycopg2.connect(dbname= 'maxi_new', host='sql.cabify.com', user=pg_user, password= pg_pass)
	cur_pg = con_pg.cursor()
	cur_pg.execute("""
		SELECT
			distinct on (agency_id) *
		FROM
			bp.referral_agency_config
		ORDER BY agency_id, created_at DESC
		""")
except Exception as e:
	slack_message(': <!channel> ERROR Unable to read current agency configurations: '+ str(e))
	exit()
agency_config = cur_pg.fetchall()
print (len(agency_config))



###FETCH CURRENT PARTICIPANTS
print ('Fetching existing participants... ', end='')
try:
	con_pg = psycopg2.connect(dbname= 'maxi_new', host='sql.cabify.com', user=pg_user, password= pg_pass)
	cur_pg = con_pg.cursor()
	cur_pg.execute('''
		SELECT
			distinct (applicant_id)
		FROM
			bp.referral_participants
		''')
except Exception as e:
	slack_message(': <!channel> ERROR Unable to read current participants: '+ str(e))
	exit()
current_applicants = cur_pg.fetchall()
print (len(current_applicants))
current_applicants_id=[i[0] for i in current_applicants]

###ADD NEW APPLICANTS
for agency in agency_config:
	print ('Fetching new valid applicants for agency '+agency[0]+': ', end='')
	agency_new = 0
	agency_duplicated= 0
	try:
		new_participants(agency[0],agency[1],agency[2],agency[3],agency[4],agency[5],agency[6],agency[7])
	except:
		slack_message(': <!channel> ERROR Unable to read new participants for agency '+agency_id)
		print('ERROR Unable to read new participants for agency '+agency_id)
		continue
	for applicant in valid_applicants:
		if applicant[0] in current_applicants_id:
			print (applicant[0] + ' Skipped: already on the program')
			duplicated = duplicated + 1
			agency_duplicated = agency_duplicated + 1
			continue
		try:
			cur_pg.execute('''
				INSERT INTO bp.referral_participants
				VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
				(applicant[0], applicant[1], applicant[2], applicant[3], applicant[4], applicant[5], applicant[6], applicant[7], applicant[8], applicant[9], applicant[10], applicant[11], applicant[12], applicant[13], applicant[14], applicant[15], applicant[16], applicant[17], applicant[18], applicant[19], applicant[20], applicant[21], applicant[22], applicant[23], applicant[24]))
			con_pg.commit()
			new = new + 1
			agency_new = agency_new + 1
			print (applicant[0] + ' applicant included', end='')
			try:
				braze_payload = "{\n  \"api_key\": \""+braze_api+"\",\n  \"campaign_id\": \"3b3e9cbd-f984-b2ad-89a0-4c8a3e3a90a4\",\n  \"recipients\": [\n     {\n      \"external_user_id\": \""+applicant[10]+"\"\n     }\n   ]\n}"
				response = requests.request("POST", url = "https://rest.iad-01.braze.com/campaigns/trigger/send", data=braze_payload, headers=braze_headers)
				print ('. Braze response:'+response.text)
			except:
				print(': <!channel> ERROR Unable to send push to godfather (new applicants)')
		except psycopg2.Error as e:
			slack_message(': <!channel> ERROR Unable to insert new participants: '+ str(e))
			exit()
	slack_message(":\nAgency {0} data.\nNew applicants: {1}\nExcluded duplicated: {2}".format(agency[0], agency_new, agency_duplicated))
slack_message(": Script loaded succesfully. Runtime: {0} seconds.\nExisting participants: {1}\nNew applicants: {2}\nExcluded duplicated: {3}".format((round(time.time() - start_time, 2)), len(current_applicants), new, duplicated))
