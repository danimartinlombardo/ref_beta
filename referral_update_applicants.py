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

###UPDATE APPLICANTS: DB
try:
	con_pg = psycopg2.connect(dbname= 'maxi_new', host='sql.cabify.com', user=pg_user, password= pg_pass)
	cur_pg = con_pg.cursor()
	cur_pg.execute("""
		UPDATE bp.referral_participants
		SET
			do_num = (data_update.do_num - data_update.do_strange_num),
			do_strange_num = data_update.do_strange_num,
    		state = (case
					when data_update.dateline_dttm < ((Now() at time zone data_update.time_zone) - Interval '9 days') then 'obsolete'
					when data_update.dateline_dttm < ((Now() at time zone data_update.time_zone) - Interval '7 days') then 'clear'
					when (data_update.do_num - data_update.do_strange_num)  >= data_update.conditions_required_do then 'achieved'
					when data_update.dateline_dttm > (Now() at time zone data_update.time_zone) then 'on_time'
					else 'expired'
				end),
    		updated_at_utc = Now(),
    		updated_at_local = timezone(data_update.time_zone, timezone('UTC', Now()))
		FROM (
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
					SUM(case 
        				when j.alerts like '%distance_too_short%' or j.alerts like '%duration_too_short%' then 1 
        				else 0
        				end) as do_strange_num
				FROM
					bp.referral_participants rf
					inner join journeys j on rf.applicant_id = j.driver_id
					inner join regions r on j.region_id = r.region_id
				WHERE
					timezone(r.time_zone, timezone('UTC', j.start_at)) < rf.dateline_dttm
					and j.end_state = 'drop off'
					and rf.state != 'obsolete'
				group by 1,2,3,4) a
      		) AS data_update
		WHERE
			referral_participants.applicant_id=data_update.applicant_id
	""")
	con_pg.commit()
except psycopg2.Error as e:
	slack_message(': <!channel> ERROR Unable to update participants data: '+ str(e))
	print(': <!channel> ERROR Unable to update participants data: '+ str(e))
	exit()
#print('Program DO & states updated')

###UPDATE APPLICANTS: BRAZE
try:
	cur_pg.execute("""	
		SELECT
			q.external_id,
			'"'||string_agg (q.fullname_quote, ', ')||'"' as referrals_name,
			'"'||string_agg (q.email_quote, ', ')||'"' as referrals_email,
			'"'||string_agg (q.dateline_quote, ', ')||'"' as referrals_dateline,
			'"'||string_agg (q.required_do_quote, ', ')||'"' as referrals_required_do,
			'"'||string_agg (q.state_quote, ', ')||'"' as referrals_state,
			'"'||string_agg (q.actual_do_quote, ', ')||'"' as referrals_actual_do,
			'"'||string_agg (q.updated_at_local_quote, ', ')||'"' as referrals_updated_at_local
		FROM
			(SELECT
				rp.godfather_id as external_id,
				(case when rp.state='clear' then NULL else rp.applicant_fullname end ) as fullname_quote,
				(case when rp.state='clear' then NULL else rp.applicant_email end) as email_quote,
				(case when rp.state='clear' then NULL else ''''||(to_char(rp.dateline_dttm,'DD/MM/YYYY'))||'''' end) as dateline_quote,
				(case when rp.state='clear' then NULL else (rp.conditions_required_do)::text end) as required_do_quote,
				(case when rp.state='clear' then NULL else rp.state end) as state_quote,
				(case when rp.state='clear' then NULL else (rp.do_num)::text end) as actual_do_quote,
				(case when rp.state='clear' then NULL else ''''||(to_char(rp.updated_at_local,'DD/MM/YYYY HH:MI'))||'''' end) as updated_at_local_quote
			FROM
				bp.referral_participants rp
			WHERE rp.state != 'obsolete'
			ORDER BY rp.godfather_id, rp.created_at_utc, rp.applicant_email) q
		GROUP BY 1;
	""")
	#print ('Braze arrays ready to upload')
except psycopg2.Error as e:
	slack_message(': <!channel> ERROR Unable to create Braze arrays: '+ str(e))
	print(': <!channel> ERROR Unable to create Braze arrays: '+ str(e))
	exit()
braze_arrays = cur_pg.fetchall()
for godfather in braze_arrays:	
	try:
		# payload as string
		braze_payload = "{\n  \"api_key\": \""+braze_api+"\",\n  \"attributes\": [ \n \t{\n \t  \"external_id\":\""+godfather[0]+"\",\n      \"referrals_name_str\": "+godfather[1]+",\n      \"referrals_email_str\": "+godfather[2]+",\n      \"referrals_dateline_str\": "+godfather[3]+",\n      \"referrals_required_do_str\": "+godfather[4]+",\n      \"referrals_state_str\": "+godfather[5]+",\n      \"referrals_actual_do_str\": "+godfather[6]+",\n      \"referrals_updated_at_local_str\": "+godfather[7]+"\n    }\n   ]\n}"
		response = requests.request("POST", url = "https://rest.iad-01.braze.com/users/track", data=braze_payload, headers=braze_headers)
		print (godfather[0] + ' Braze attributes updated. Response '+response.text)
	except:
		slack_message(': <!channel> ERROR Braze attributes update error')
		print(': <!channel> ERROR Braze attributes update error')
		
slack_message(": Script loaded succesfully. Runtime: %s seconds" % round(time.time() - start_time, 2))
