select 
	distinct 
	stellaconnect_responses.id,
	stellaconnect_responses.client_account,
	request_id,
	ext_interaction_id,
	channel,
	star_rating,
	employee_first_name,
	employee_last_name,
	employee_email,
	team_leader,
	customer_name,
	customer_email,
	reward,
	comments,
	areas_for_improvement,
	tags,
	to_char(request_sent_at_date::date,'mm/dd/yyyy') as request_sent_at_date,
	request_sent_at_utc_time,
	to_char(response_received_at_date::date,'mm/dd/yyyy') as  response_received_at_date,
	stellaconnect_responses.timestamp,
	agent_name
from 
	stellaconnect_responses
left join sd_client_agent_roster sar on employee_email = agent_email
where 
	stellaconnect_responses.client_account in ('brooklinen')
	and employee_email like '%boldrimpact%' 
order by 
	timestamp desc,
	request_sent_at_utc_time
limit 10000
