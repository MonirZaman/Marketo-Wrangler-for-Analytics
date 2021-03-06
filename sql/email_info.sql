/*
	Query provides (# sent, opened, unsubscribed) information for an email
*/

declare				-- NOTE: EMAIL SHOULD BE WITHIN 2017-10-4 TO 2017-10-25
	@program_name varchar(100) = '%ENTER EMAIL HERE%'; 

with program_log as (
  select * --distinct [New Status], [Old Status], [activityTypeName]
  FROM [YOUR_SCHEMA].[dbo].[ACTIVITY_TABLE]
  where primaryAttributeValue like @program_name
),

program_distinct_status as (
  select distinct [activityTypeName]
  FROM [YOUR_SCHEMA].[dbo].[ACTIVITY_TABLE]
  where primaryAttributeValue like @program_name
),


count_sent as (
	select 
			count(1) counts,
			'Sent' count_type
	from (
		select distinct leadId
		from program_log
		where [activityTypeName]  in ('Send Email')
	) t
),

count_opened as (
	select 
			count(1) counts,
		   'Opened' count_type
	from (
		select distinct leadId
		from program_log
		where [activityTypeName]  in ('Open Email') -- not in ('Email Bounced', 'Email Bounced Soft')
	) t
),

count_unsubscribe as (
	select 
			count(1) counts,
		   'Unsubscribed' count_type
	from (
		select distinct leadId
		from program_log
		where [activityTypeName]  in ('Unsubscribe Email') -- not in ('Email Bounced', 'Email Bounced Soft')
	) t
),

count_click_email as (
	select 
			count(1) counts,
		   'Click Email' count_type
	from (
		select distinct leadId
		from program_log
		where [activityTypeName]  in ('Click Email') -- not in ('Email Bounced', 'Email Bounced Soft')
	) t
),

count_invited as (
	select 
			count(1) counts,
		   'Invited' count_type
	from (
		select distinct leadId
		from program_log
		where activityTypeName = 'Send Email'
			
	) t
),

all_counts as (
	select *
	from count_sent
	
	union 
	
	select *
	from count_opened 

	union

	select *
	from count_unsubscribe

	union

	select *
	from count_click_email
)

select *
from all_counts 
