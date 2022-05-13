select 
		to_char(local_date_created::date,'mm/dd/yyyy') as local_date_created,
		assignee_id,
		email_address,
		mc_count,
		daily_total::varchar,
		weekly_total::varchar,
		monthly_total::varchar,
		cx_name 
from 
	(
		select
				distinct 
				local_date_created,
				agent_name as assignee_id,
				email_address,
				case 
					when mc_count is null 
					then 0 
					else mc_count
				end as mc_count,
				daily_total,
				weekly_total,
				monthly_total,
				cx_name
			from 
				(
					select 
						local_date_created,
						agent_name,
						email_address,
						sum_daily,
						mc_count,
						cx_name
					from (
					select 
						distinct 
						local_date_created,
						agent_name,
						email_address,
						count(daily_chats) over (partition by local_date_created, agent_name,email_address) as sum_daily
					from 
					(
						select 
							left(local_timestamp::varchar,10)::date as local_date_created,
							display_name as agent_name,
							email as email_address,
							count(*) over (partition by chat_id,display_name,email) as daily_chats
						from 
						(
						select 
							chat_id,
							display_name,
							email,
							timestamp+'8:00' as local_timestamp
						from livechat_chat_operators 
						) a 
					) a ) b
					left join
					(
					select 
						distinct
						local_date,
						local_timestamp,
						display_name,
						email,
						cx_name,
						mc_count
					from
						(select 
						*
					from 
					(select 
						livechat_chat_operators.chat_id,
						timestamp+'8:00' as local_timestamp,
						left((timestamp+'8:00')::varchar,10)::date as local_date,
						display_name,
						email,
						operators_count,
						author_name as cx_name,
						count(*) over (partition by chat_id, display_name) as mc_count
					from 
						livechat_chat_operators
					join
					(
					select
						distinct 
						chat_id as q_id,
						author_name
					from 
					(
					select 
						*,
						max(message_count) over (partition by chat_id) as max_m 
					from 
						livechat_chat_messages
					) a 
					where 
						max_m = message_count 
						and user_type = 'visitor'
						and message_text like '%?'
					) a 
					on chat_id = q_id
					where email like '%boldr%'
					) a ) b ) c
					on local_date = local_date_created and display_name = agent_name
				) as check_livechat
				left join
					(select 
						distinct 
						local_date,
						count(chat_id) over (partition by local_date) as daily_total,
						date(date_trunc('week',local_date)) as week_date,
						count(chat_id) over (partition by date(date_trunc('week',local_date))) as weekly_total,
						date(date_trunc('month',local_date)) as month_date,
						count(chat_id) over (partition by date(date_trunc('month',local_date))) as monthly_total
					from (
					select 
							distinct
							livechat_chat_operators.chat_id,
							start_timestamp + '8:00' as local_timestamp,
							left((start_timestamp + '8:00')::varchar,10)::date as local_date
						from livechat_chat_operators 
						left join livechat_chat_insights
						on livechat_chat_operators.chat_id = livechat_chat_insights.chat_id
					) a ) as total_ 
					on local_date = local_date_created 
		) as sub_null_to_zero
order by local_date_created::date desc
limit 10000
