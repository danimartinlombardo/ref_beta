import psycopg2
import os
import sys
import requests
from credentials import *
from config_CO import *

start_time = time.time()

braze_headers = slack_headers = {'Content-Type': "application/json", 'Cache-Control': "no-cache"}

def slack_message (text):
	slack_payload = "{\n\t\"text\": \""+os.path.basename(__file__)+text+"\"\n}"
	slack = requests.request("POST", slack_url, data=slack_payload, headers=slack_headers)	

os.system('clear')

###FETCH CURRENT PARTICIPANTS
#print ('Fetching existing participants... ', end='')
try:
	con_pg = psycopg2.connect(dbname= 'maxi_new', host='sql.cabify.com', user=pg_user, password= pg_pass)
	cur_pg = con_pg.cursor()
	cur_pg.execute('''
		SELECT
			distinct (applicant_id)
		FROM
			bp.referral_participants_temp
		''')
except Exception as e:
	slack_message(': <!channel> ERROR Unable to read current participants: '+ str(e))
	exit()
current_applicants = cur_pg.fetchall()
#print (len(current_applicants))
current_applicants_id=[i[0] for i in current_applicants]
###INSERT NEW APPLICANTS
#print ('Fetching new valid applicants... ', end='')
try:
	con_rs=psycopg2.connect(dbname= 'dwh', host='cabify-datawarehouse.cxdpjwjwbg9i.eu-west-1.redshift.amazonaws.com', port= '5439', user= rs_user, password= rs_pass)
	cur_rs= con_rs.cursor()
	cur_rs.execute('''
		SELECT
			applicant.id_driver as applicant_id,
			applicant2.ds_email as applicant_email,
			applicant.ds_name||' '||applicant.ds_surname as applicant_fullname,
			j.id_journey as first_do_journey_id,
			min_do.tm_start_local_at as first_do_local_dttm,
			DATEADD(week, 4, min_do.tm_start_local_at) as dateline_dttm,
			r.id_region as first_do_region_id,
			r.ds_time_zone as time_zone,
			a.id_agency as first_do_agency_id,
			ad.ds_driver_invitation_code as applicant_code,
			--godfather.id_user as godfather_id,
			'e890e087420df9a537a7d070e9c69fa6' as godfather_id,
			0 as do_num,
			'on_time' as state,
			%s as conditions_week_num,
			%s as conditions_required_do,
			%s as conditions_amount_granted_godfather,
			%s as conditions_amount_granted_applicant,
			NULL as bonus_request_id,
			getdate() as created_at_utc,
			getdate() as updated_at_utc,
			null as updated_at_local
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
			j.dt_start_local_at = date_trunc('day', DATEADD(day, -1, GETDATE()))
			and a.id_agency IN ('33f0e9373e981d2425d4da8d005a610b') /*CO*/
		''',(week_num_limit,week_num_limit,required_do_num,amount_granted_godfather, amount_granted_applicant))
except psycopg2.Error as e:
	slack_message(': <!channel> ERROR Unable to read new participants: '+ str(e))
	exit()
valid_applicants = cur_rs.fetchall()
print (len(valid_applicants))

for applicant in valid_applicants:
	if applicant[0] in current_applicants_id:
		#print (applicant[0] + ' Skipped: already on the program')
		continue
	try:
		cur_pg.execute('''
			INSERT INTO bp.referral_participants_temp
			VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
			(applicant[0], applicant[1], applicant[2], applicant[3], applicant[4], applicant[5], applicant[6], applicant[7], applicant[8], applicant[9], applicant[10], applicant[11], applicant[12], applicant[13], applicant[14], applicant[15], applicant[16], applicant[17], applicant[18], applicant[19], applicant[20]))
		con_pg.commit()
		#print (applicant[0] + ' applicant included', end='')
		try:
			braze_payload = "{\n  \"api_key\": \""+braze_api+"\",\n  \"campaign_id\": \"3b3e9cbd-f984-b2ad-89a0-4c8a3e3a90a4\",\n  \"recipients\": [\n     {\n      \"external_user_id\": \""+applicant[9]+"\"\n     }\n   ]\n}"
			response = requests.request("POST", url = "https://rest.iad-01.braze.com/campaigns/trigger/send", data=braze_payload, headers=braze_headers)
			#print ('. Braze response:'+response.text)
		except:
			print(': <!channel> ERROR Unable to send push to godfather (new applicants)')

	except psycopg2.Error as e:
		slack_message(': <!channel> ERROR Unable to insert new participants: '+ str(e))
		exit()

slack_message(": Script loaded succesfully. Runtime: %s seconds" % round(time.time() - start_time, 2))')