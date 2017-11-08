## Tables
    * Leads - stores constituent/lead information
    * Program - stores all the program information
    * Activity - stores all the activity log
    

## Get list of emails and events

Use the column `Program.channel` and subset to the following values to get the list of emails and events.

|channel|
|-------:|
|Email|
|Email Blast|
|Subscription|
|Webinar|
|Fundraising|
|Nurture|
|Standard Event|


## Get membership information of a event
An event (denoted by :date:) is a program. 
`[Program].[name] -> [Activity].[primaryAttributeValue]`

The following query will provide all the membership information of the event.
```
SELECT 
    *
  FROM [Marketo].[dbo].[Activity]
  where [primaryAttributeValue] like '2017-10-18 DEV: Webinar Helping your child plan for post-secondary education'
```

## Get log of individual email that belong to an event
An event may contain multiple emails. 
`[Program].[name] -> [Activity].[primaryAttributeValue].Individual email name`

```
SELECT 
    *
  FROM [Marketo].[dbo].[Activity]
  where [primaryAttributeValue] like '2017-10-18 DEV: Webinar Helping your child plan for post-secondary education.OE20%'
```

## How to get email information that is not part of an event
An email (denoted by :mailbox_with_no_mail:) can have one or emails associated with them. However, Program table will contain only one record describing the parent email. Activity table will contain logs related to sub email. Therefore, we use % character at the end of the email name in the condition. E.g.,  

```
SELECT 
    *
FROM [Marketo].[dbo].[Activity]
WHERE [primaryAttributeValue] like 'Psychology Clinic%'
```