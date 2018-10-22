import psycopg2
import os
import requests
from credentials import *
from config_CO import *

braze_headers = slack_headers = {'Content-Type': "application/json", 'Cache-Control': "no-cache"}

def slack_message (text):
	slack_payload = "{\n\t\"text\": \""+text+"\"\n}"
	slack = requests.request("POST", slack_url, data=slack_payload, headers=slack_headers)	

os.system('clear')

#FETCH CURRENT PARTICIPANTS
print ('Fetching existing participants... ', end='')
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
	print('Unable to read current participants: '+ str(e))
	slack_message('Unable to read current participants: '+ str(e))
	exit()
current_applicants = cur_pg.fetchall()
print (len(current_applicants))
current_applicants_id=[i[0] for i in current_applicants]

#INSERT NEW APPLICANTS
print ('Fetching new valid applicants... ', end='')
try:
	con_rs=psycopg2.connect(dbname= 'dwh', host='cabify-datawarehouse.cxdpjwjwbg9i.eu-west-1.redshift.amazonaws.com', port= '5439', user= rs_user, password= rs_pass)
	cur_rs= con_rs.cursor()
	cur_rs.execute('''
		SELECT
			u.user_id as applicant_id,
			u.user_email as applicant_email,
			u.user_fullname as applicant_fullname,
			d.min_do_journey_id as first_do_journey_id,
			agd.first_do_start_at_local_dttm as first_do_local_dttm,
			DATEADD(week, %s, agd.first_do_start_at_local_dttm) as dateline_dttm,
			r.region_id as first_do_region_id,
			r.region_time_zone as time_zone,
			r.agency_id as first_do_agency_id,
			ad.ds_driver_invitation_code as applicant_code,
			--gf.user_id as godfather_id,
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
			dwh.agg_drivers agd
			inner join dwh.dim_driver d on agd.driver_sk = d.driver_sk
			inner join dwh.t_dim_user u on d.user_sk = u.user_sk
			inner join dwh.t_dim_region r on agd.first_do_region_sk = r.region_sk
			inner join datawarehouse.ops_dim_driver dd on u.user_id = dd.id_driver
			inner join datawarehouse.lgt_fac_applicantdetail ad on dd.fk_applicant_id = ad.sk_applicantdetail
			inner join dwh.t_dim_user gf on lower(trim(ad.ds_driver_invitation_code)) = lower(trim(gf.user_email))
		--WHERE
			--r.agency_id IN ('33f0e9373e981d2425d4da8d005a610b') /*CO*/
			--d.min_do_start_at_utc_dt = date_trunc('day', DATEADD(day, -1, GETDATE() ))
			--date_trunc('month',agd.first_do_start_at_local_dttm) in ('2018-09-01','2018-08-01','2018-07-01')
		--limit 2
			''',(week_num_limit,week_num_limit,required_do_num,amount_granted_godfather, amount_granted_applicant))
except psycopg2.Error as e:
	print('Unable to read new participants: '+ str(e))
	slack_message('Unable to read new participants: '+ str(e))
	exit()
valid_applicants = cur_rs.fetchall()
print (len(valid_applicants))

for applicant in valid_applicants:
	if applicant[0] in current_applicants_id:
		print (applicant[0] + ' Skipped: already on the program')
		continue
	try:
		cur_pg.execute('''
			INSERT INTO bp.referral_participants_temp
			VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
			(applicant[0], applicant[1], applicant[2], applicant[3], applicant[4], applicant[5], applicant[6], applicant[7], applicant[8], applicant[9], applicant[10], applicant[11], applicant[12], applicant[13], applicant[14], applicant[15], applicant[16], applicant[17], applicant[18], applicant[19], applicant[20]))
		con_pg.commit()
		print (applicant[0] + ' applicant included', end='')
		try:
			braze_payload = "{\n  \"api_key\": \""+braze_api+"\",\n  \"campaign_id\": \"3b3e9cbd-f984-b2ad-89a0-4c8a3e3a90a4\",\n  \"recipients\": [\n     {\n      \"external_user_id\": \""+applicant[9]+"\"\n     }\n   ]\n}"
			response = requests.request("POST", url = "https://rest.iad-01.braze.com/campaigns/trigger/send", data=braze_payload, headers=braze_headers)
			print ('. Braze response:'+response.text)
		except:
			print('. Error while sending comm')

	except psycopg2.Error as e:
		print('Unable to insert new participants: '+ str(e))
		slack_message('Unable to insert new participants: '+ str(e))
		exit()

#UPDATE APPLICANTS: DB
try:
	cur_pg.execute('''
		UPDATE bp.referral_participants_temp
		SET
			do_num = data_update.do_num,
    		state = (case
					when data_update.dateline_dttm < ((Now() at time zone data_update.time_zone) - Interval '9 days') then 'obsolete'
					when data_update.dateline_dttm < ((Now() at time zone data_update.time_zone) - Interval '7 days') then 'clear'
					when data_update.do_num >= data_update.conditions_required_do then 'achieved'
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
				a.conditions_required_do
			FROM (
				SELECT
					rf.applicant_id,
					rf.dateline_dttm,
					rf.conditions_required_do,
					r.time_zone,
					count(j.journey_id) as do_num
				FROM
					bp.referral_participants_temp rf
					inner join journeys j on rf.applicant_id = j.driver_id
					inner join regions r on j.region_id = r.region_id
				WHERE
					timezone(r.time_zone, timezone('UTC', j.start_at)) < rf.dateline_dttm
					and j.end_state = 'drop off'
					and rf.state != 'obsolete'
				group by 1,2,3,4) a
      		) AS data_update
		WHERE
			referral_participants_temp.applicant_id=data_update.applicant_id
	''', (required_do_num,))
	con_pg.commit()
except psycopg2.Error as e:
	print('Unable to update participants data: '+ str(e))
	slack_message('Unable to update participants data: '+ str(e))
	exit()
print('Program DO & states updated')

#UPDATE APPLICANTS: BRAZE
try:
	#using strings 
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
				bp.referral_participants_temp rp
			WHERE rp.state != 'obsolete'
			ORDER BY rp.godfather_id, rp.created_at_utc, rp.applicant_email) q
		GROUP BY 1;
	""")
	print ('Braze arrays ready to upload')
except psycopg2.Error as e:
	print('Unable to create Braze arrays: '+ str(e))
	slack_message('Unable to create Braze arrays: '+ str(e))
	exit()
braze_arrays = cur_pg.fetchall()
for godfather in braze_arrays:	
	try:
		# payload as string
		print (godfather[3])
		print (godfather[7])
		braze_payload = "{\n  \"api_key\": \""+braze_api+"\",\n  \"attributes\": [ \n \t{\n \t  \"external_id\":\""+godfather[0]+"\",\n      \"referrals_name_str\": "+godfather[1]+",\n      \"referrals_email_str\": "+godfather[2]+",\n      \"referrals_dateline_str\": "+godfather[3]+",\n      \"referrals_required_do_str\": "+godfather[4]+",\n      \"referrals_state_str\": "+godfather[5]+",\n      \"referrals_actual_do_str\": "+godfather[6]+",\n      \"referrals_updated_at_local_str\": "+godfather[7]+"\n    }\n   ]\n}"
		print(braze_payload)
		response = requests.request("POST", url = "https://rest.iad-01.braze.com/users/track", data=braze_payload, headers=braze_headers)
		print (godfather[0] + ' Braze attributes updated. Response '+response.text)
	except:
		print ('Braze attributes update error')
		slack_message('Braze attributes update error')
		
slack_message('Referrals script ran succesfully')
