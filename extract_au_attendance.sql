select
	agent_name,
	agent_email, 
	case when supervisor = 'Team Total' then 'Vicky Hancock' else supervisor end as supervisor,
	to_char(shift_start,'mm/dd/yyyy') as local_date, 
	to_char(shift_start,'mm/dd/yyyy hh24:mi:ss') as shift_start, 
	to_char(shift_end,'mm/dd/yyyy hh24:mi:ss') as shift_end, 
	to_char(clock_in,'mm/dd/yyyy hh24:mi:ss') as clock_in, 
	to_char(clock_out,'mm/dd/yyyy hh24:mi:ss') as clock_out, 
	absenteeism::varchar, 
	tardiness::varchar
from 
	d_attendance_kpi_summary daks  
left join (select distinct agent_email as email_ from sd_client_agent_roster where client_account = 'brooklinen') sar on sar.email_ = agent_email 
where 
	client_account = 'brooklinen' or client_account = 'UPlevel'
order by shift_start::date desc
limit 10000
