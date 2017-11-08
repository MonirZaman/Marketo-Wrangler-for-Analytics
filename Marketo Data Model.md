# Marketo Data Model

## Tables
For security reasons, only last part of the table name is mentioned here.  

    * Leads  
    	- Stores constituent/lead information
    * Program  
    	- Stores all the program information including Email and Events
    * Activity  
    	- Stores all the activity log
    * Campaigns  
    	- Stores individual email information that are part of an event


## JOIN

* Program : Activity  
`Program.name = Activity.primaryAttributeValue`

* Program : Campaigns  
`Program.Id = Campaigns.Id`

* Leads : Activity  
`Leads.Id=Activity.leadId`

## Get list of emails and events

Use the column `Program.channel` and subset to the following values to get the list of emails and events.

|channel|
|:-------:|
|Email|
|Email Blast|
|Subscription|
|Webinar|
|Fundraising|
|Nurture|
|Standard Event|


## EVENT
An event (denoted by :date:) is considered as a program. One or more emails can be part of an event. E.g.,
![Event structure](image/event_structure.JPG)


The following query will provide all the membership information of the event.
```
SELECT 
    *
  FROM [Marketo].[dbo].[Activity]
  where [primaryAttributeValue] like '2017-10-18 DEV: Webinar Helping your child plan for post-secondary education'
```

### Count event's attendance 
The following query will provide the number of attendee of all the events.

```
--NOTE: Schema and table names are dummy for security reasons. Replace dummy names

select
	[Program Id],
	[Program Name],
	count(1) [Count attendee]

from (
	select 
		distinct 
			 p.id 'Program Id',
			 p.name 'Program Name', 
			 a.leadId
	FROM [Marketo].[dbo].[Program] p
		inner join [Marketo].[dbo].[Activity] a
			on p.name = a.primaryAttributeValue

	where p.channel in ('Webinar', 'Standard Event')
		and a.activityTypeName = 'Change Status in Progression'
		and ([New Status] like 'Attended%' or [Old Status] like 'Attended%') 

) program_lead

group by [Program Id], [Program Name]
```


### Get log of individual email that belong to an event
To obtain information regarding individual email that belong to an event, use email name followed by the event name in the following format. 
`EVENT NAME.EMAIL NAME`

Below is a sample query. 

```
SELECT 
    *
  FROM [Marketo].[dbo].[Activity]
  where [primaryAttributeValue] like '2017-10-18: Webinar Helping post-secondary education.OE20'
```

## EMAIL  
An email (denoted by :mailbox_with_no_mail:) can have one or emails associated with them. 
![Email structure](image/email_structure.JPG)

However, Program table will contain only one record describing the parent email. Activity table will contain logs related to sub individual emails. Email name will be in the format `PARENT EMAIL NAME.CHILD EMAIL NAME`. In order to get all logs related to the Email program, use the following query.



```
SELECT 
    *
FROM [Marketo].[dbo].[Activity]
WHERE [primaryAttributeValue] like 'Psychology Clinic%'
```

## Comments
- Not all the email program will have records in the `Campaign` table
- An email program has records in the `Campaign` table if it contains an item of type `Campaign`
