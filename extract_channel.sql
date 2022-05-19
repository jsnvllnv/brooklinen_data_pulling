select 
	*
		from
			((select 
				to_char(start_date::date,'mm/dd/yyyy') as start_date,
				period,
				agent_name,
				agent_email,
				supervisor,
				activity,
				total_duration,
				total_duration_hr,
				hr_duration,
				channel_group
					from
						(select  
							*,
							concat(trunc(total_duration_hr),':',(case when floor((((total_duration_hr)-trunc(total_duration_hr))*60))<10 then (concat('0',floor((((total_duration_hr)-trunc(total_duration_hr))*60))::numeric))::varchar else (floor((((total_duration_hr)-trunc(total_duration_hr))*60)))::varchar end)) as hr_duration,
							string_agg(activity,'-') over (partition by agent_name, start_date, supervisor order by activity) as channel_group
								from
									(select 
										distinct
										start_date,
										'Daily' as period,
										agent_name,
										agent_email,
										supervisor,
										activity,
										sum(duration_seconds) over (partition by start_date, agent_name, supervisor, activity) as total_duration,
										(sum(duration_seconds) over (partition by start_date, agent_name, supervisor, activity))/3600 as total_duration_hr
									from 
										sd_utilization su 
									where 
										division_name = 'Brooklinen'
										and (activity = 'Chat' or activity = 'Email' or activity = 'Voice')
									order by 
										start_date,agent_name,activity ) as au ) as daily_group
				where total_duration is not null
				order by start_date desc, agent_name, activity)
			union all 
			(select 
				to_char(start_date::date,'mm/dd/yyyy') as start_date,
				period,
				agent_name,
				agent_email,
				supervisor,
				activity,
				total_duration,
				total_duration_hr,
				hr_duration,
				string_agg(channel_group_w,'-') over (partition by agent_name, start_date, supervisor) as channel_group
					from
						(select 
							start_date,
							period,
							agent_name,
							agent_email,
							supervisor,
							activity,
							total_duration,
							total_duration_hr,
							hr_duration,
							case when rn = 1 then channel_group_w else null end as channel_group_w
								from 
									(select 
										*,
										row_number() over ( partition by agent_name, start_date, supervisor order by char_length(channel_group_w) desc) as rn
											from
												(select  
													*,
													concat(trunc(total_duration_hr),':',(case when floor((((total_duration_hr)-trunc(total_duration_hr))*60))<10 then (concat('0',floor((((total_duration_hr)-trunc(total_duration_hr))*60))::numeric))::varchar else (floor((((total_duration_hr)-trunc(total_duration_hr))*60)))::varchar end)) as hr_duration,
													string_agg(activity,'-') over (partition by agent_name, start_date, supervisor order by activity) as channel_group_w
														from
															(select 
																distinct
																date(date_trunc('week',start_date)) as start_date,
																'Weekly' as period,
																agent_name,
																agent_email,
																supervisor,
																activity,
																sum(duration_seconds) over (partition by date(date_trunc('week',start_date)), agent_name, supervisor, activity) as total_duration,
																(sum(duration_seconds) over (partition by date(date_trunc('week',start_date)), agent_name, supervisor, activity))/3600 as total_duration_hr
															from 
																sd_utilization su 
															where 
																division_name = 'Brooklinen'
																and (activity = 'Chat' or activity = 'Email' or activity = 'Voice')
															order by 
																start_date,agent_name,activity ) as au ) as daily_group ) as c_length ) as final_channel_m
				order by start_date desc, agent_name)
			union all 
			(select 
				to_char(start_date::date,'mm/dd/yyyy') as start_date,
				period,
				agent_name,
				agent_email,
				supervisor,
				activity,
				total_duration,
				total_duration_hr,
				hr_duration,
				string_agg(channel_group_m,'-') over (partition by agent_name, start_date, supervisor) as channel_group
					from
						(select 
							start_date,
							period,
							agent_name,
							agent_email,
							supervisor,
							activity,
							total_duration,
							total_duration_hr,
							hr_duration,
							case when rn = 1 then channel_group_m else null end as channel_group_m
								from 
									(select 
										*,
										row_number() over ( partition by agent_name, start_date, supervisor order by char_length(channel_group_m) desc) as rn
											from
												(select  
													*,
													concat(trunc(total_duration_hr),':',(case when floor((((total_duration_hr)-trunc(total_duration_hr))*60))<10 then (concat('0',floor((((total_duration_hr)-trunc(total_duration_hr))*60))::numeric))::varchar else (floor((((total_duration_hr)-trunc(total_duration_hr))*60)))::varchar end)) as hr_duration,
													string_agg(activity,'-') over (partition by agent_name, start_date, supervisor  order by activity) as channel_group_m
														from
															(select 
																distinct
																date(date_trunc('month',start_date)) as start_date,
																'Monthly' as period,
																agent_name,
																agent_email,
																supervisor,
																activity,
																sum(duration_seconds) over (partition by date(date_trunc('month',start_date)), agent_name, supervisor, activity) as total_duration,
																(sum(duration_seconds) over (partition by date(date_trunc('month',start_date)), agent_name, supervisor, activity))/3600 as total_duration_hr
															from 
																sd_utilization su 
															where 
																division_name like '%Brooklinen%'
																and (activity = 'Chat' or activity = 'Email' or activity = 'Voice')
															order by 
																start_date,agent_name,activity ) as au ) as daily_group ) as c_length ) as final_channel_m
				order by start_date desc, agent_name)
			) as all_
	order by start_date::date desc
	limit 20000
