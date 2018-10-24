import psycopg2
import os, sys
import requests
import time
from credentials import *
from config_CO import *

start_time = time.time()

braze_headers = slack_headers = {'Content-Type': "application/json", 'Cache-Control': "no-cache"}

def slack_message (text):
	slack_payload = "{\n\t\"text\": \""+os.path.basename(__file__)+text+"\"\n}"
	slack = requests.request("POST", slack_url, data=slack_payload, headers=slack_headers)	

os.system('clear')

###FETCH NEW BONUS DATA
try:
	con_pg = psycopg2.connect(dbname= 'maxi_new', host='sql.cabify.com', user=pg_user, password= pg_pass)
	cur_pg = con_pg.cursor()
	cur_pg.execute('''
			SELECT
				rf.godfather_id||'_'||rf.applicant_id||'_referrer' as referral_bonus_id,
				rf.first_do_region_id as region_id,
				rf.timezone,
				rf.godfather_id as driver_id,
				rf.conditions_amount_granted_godfather as amount,
				%s as currency,
				%s as currency_factor,
				%s as tax_code,
				date_trunc('day', Now())::date as amount_dated_at,
				'Referral program. Referrer: '||rf.godfather_id||'. Applicant: '||rf.applicant_id as notes,
				'Programa de referidos: referido '||rf.applicant_fullname||' ('||rf.applicant_email||')' as explanation,
				'referral' as category,
				Now() as created_at_utc,
				rf.applicant_id as applicant_id
			FROM
				bp.referral_participants rf
			WHERE
				rf.state = 'achieved'
				and bonus_request_id IS NULL
		''', (currency,currency_factor, tax_code))
except psycopg2.Error as e:
	print('Unable read bonus request data: '+ str(e))
	slack_message(': <!channel> ERROR Unable read bonus request data: '+ str(e))
	exit()
requests = cur_pg.fetchall()
###INSERT NEW BONUS DATA & COMMS
for request in requests:
	try:
		con_pg_google = psycopg2.connect(dbname= 'bonuses_production', host='35.187.79.123', user=pg_google_user, password= pg_google_pass)
		cur_pg_google = con_pg_google.cursor()
		cur_pg_google.execute('''
				INSERT INTO production.referral_bonus_request(
	    referral_bonus_id, region_id, timezone, driver_id, amount, currency, currency_factor, tax_code, amount_dated_at, notes, explanation, category, created_at_utc)
				VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
				(request[0], request[1], request[2], request[3], request[4], request[5], request[6], request[7], request[8], request[9], request[10], request[11], request[12]))
		con_pg_google.commit()
		print ('Bonus requests data inserted. ', end='')	
		try:
			braze_payload = "{\n  \"api_key\": \""+braze_api+"\",\n  \"campaign_id\": \"183d9996-4ca0-4987-8cae-f6a344df57b9\",\n  \"recipients\": [\n     {\n      \"external_user_id\": \""+request[3]+"\"\n     }\n   ]\n}"
			response = requests.post(url = "https://rest.iad-01.braze.com/campaigns/trigger/send", data=braze_payload, headers=braze_headers)
			print ('Braze response:'+response.text)
		except:
			print('Braze error')
			slack_message(': <!channel> ERROR Bonus granted comms not working')
	except psycopg2.Error as e:
		print('Unable to insert bonus requests data: '+ str(e))
		slack_message(': <!channel> ERROR Unable to insert bonus requests data: '+ str(e))
		exit()

###UPDATE BONUS ID ON PARTICIPANTS
	try:
		cur_pg.execute('''
			UPDATE bp.referral_participants
			SET
				bonus_request_id = %s,
	    		updated_at_utc = Now()
	    	/*FROM (
	    		SELECT
	    			referral_bonus_id,
	    			split_part(referral_bonus_id, '_', 2) as applicant_id
	    		FROM	
	    			bp.referral_bonus_request
	    			) b*/
	    	WHERE referral_participants.applicant_id = %s;
			''',(request[0],request[13]))
		con_pg.commit()
		print ('Bonus id updated.')
	except psycopg2.Error as e:
		print('Unable to update bonus request id: '+ str(e))
		slack_message(': <!channel> ERROR Unable to update bonus request id: '+ str(e))
		exit()
slack_message(": Script loaded succesfully. Runtime: {0} seconds.\nNew bonus requests: {1}\n".format((round(time.time() - start_time, 2)), len(requests)))




# QUERY JUANMA
# CREATE TABLE  production.referral_bonus_request (
#     id bigserial NOT NULL,
#     referral_bonus_id character varying(255) NOT NULL,
#     region_id character varying(255) NOT NULL,
#     timezone character varying(255) NOT NULL,
#     driver_id character varying(255) NOT NULL,
#     amount integer,
#     currency character varying(255) NOT NULL,
#     currency_factor integer NOT NULL,
#     tax_code character varying(255) NOT NULL DEFAULT ''::character varying,
#     amount_dated_at date,
#     notes text,
#     explanation text,
#     category character varying(255) NOT NULL,
#     created_at_utc timestamp without time zone NOT NULL,
#     sended_to_couch character varying(255),
#     couchdb_id character varying(255),
#     CONSTRAINT referral_bonus_pkey PRIMARY KEY (id));
