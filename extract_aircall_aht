select 
	to_char(started_at::date,'mm/dd/yyyy') as local_date_created,
	extract(hour from started_at) as hour_started,
	agent_name,
	duration_call::varchar
		from
			(select 
				call_id,
				left(user_id,6)::varchar as user_id,
				started_at,
				date_started_local,
				time_started_local,
				direction,
				duration_total,
				duration_api,
				duration_call,
				user_name,
				user_email,
				agent_name,
				agent_email,
				supervisor
					from 
						aircall_phone_calls
					left join (select * from sd_core_team_brooklinen where core_team = true) sctb on (user_email = agent_email and left(user_id,6)::varchar = aircall_id::varchar)
				where 
					left(user_id,6)::varchar is not null
					and direction = 'inbound' and status = 'done'
					and duration_api is not null
					and line = 'Brooklinen CX Team NEW'
					and time_started_local between '08:00:00' and '21:00:00'
					and missed_call_reason is null) as aircall_raw
	where agent_name is not null
	order by started_at desc
