/*
	Query provides (# invited, registered, attended) information for an event
*/


declare                        -- NOTE: EVENT SHOULD BE WITHIN CRAWLED data period
	@program_name varchar(100) = '%ENTER EVENT INFO HERE%';  --Enter the event name

with program_log as (
  select * --distinct [New Status], [Old Status], [activityTypeName]
  FROM [YOUR_SCHEMA].[dbo].[ACTIVITY_TABLE]
  where primaryAttributeValue like @program_name
),

program_distinct_status as (
  select distinct [New Status], [Old Status], [activityTypeName]
  FROM [YOUR_SCHEMA].[dbo].[ACTIVITY_TABLE]
  where primaryAttributeValue like @program_name
),


count_attended as (
	select 
			count(1) counts,
			'Attended' count_type
	from (
		select distinct leadId
		from program_log
		where [New Status] = 'Attended'  
		or [Old Status] = 'Attended'
	) t
),

count_registered as (
	select 
			count(1) counts,
		   'Registered' count_type
	from (
		select distinct leadId
		from program_log
		where [New Status] = 'Registered'
			or [Old Status] = 'Registered'  
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
	from count_attended
	
	union 
	
	select *
	from count_registered 

	union 
	
	select *
	from count_invited 
)

select *
from all_counts 
