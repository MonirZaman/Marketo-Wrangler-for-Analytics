![logo](https://blog.marketo.com/favicon.png)
# Wrangling Marketo data for Analytics
Marketo provides a RESTful API to create and retrieve records. API documentation can be found [here](http://developers.marketo.com/rest-api/). In order to download records in bulk, one needs to perform multiple steps such request token, create task, start task, check status of a task and download file. Marketo Wrangler have automated the entire process and is built using Python scripts. By executing the scripts, we can download records for several months. This documentation introduces commands to make individual API requests in Section 1. Then it shows commands to automate the bulk download process using the Marketo Wrangler in Section 2. Section 3 introduces commands to import the downloaded records into a database table.


## 1. Access Marketo API using CURL

### 1.1 Individual request

#### I. Request access token
`curl -X POST -d "client_id=YOUR_ID&client_secret=YOUR_SECRET&grant_type=client_credentials&username=YOUR_EMAIL" https://123.mktorest.com/identity/oauth/token`

#### II. Get a given Lead information using access token
`curl --header "Authorization: Bearer YOUR_ACCESS_TOKEN" https://123.mktorest.com/rest/v1/lead/29086.json`

Sample response:
```
{"requestId":"abc123",
 "result":[
         {"id":29086,
          "updatedAt":"2016-03-08T17:02:39Z",
          "lastName":"Van Diesel",
          "email":"s***@viewpointgroup.ca",
          "createdAt":"2015-09-25T17:36:58Z",
          "firstName":"Mac"}
          ],
"success":true
}
```

#### III. Get a given Lead information with additional fields
`curl --header "Authorization: Bearer YOUR_ACCESS_TOKEN" https://123.mktorest.com/rest/v1/lead/123456.json?fields=id,email,firstName,lastName,mktoName,annualRevenue`

### 1.2 Bulk extract 
Bulk extract is available Lead and Activity type. Here is an example of Bulk extract of leads information.

#### I. Create bulk extract job    
`curl --header "Authorization: Bearer YOUR_ACCESS_TOKEN" -X POST -H "Content-Type: application/json" -d @body.json https://123.mktorest.com/bulk/v1/leads/export/create.json`

Response will give us an export id aka job id.

#### II. Start the job
`curl --header "Authorization: Bearer YOUR_ACCESS_TOKEN" -X POST https://123.mktorest.com/bulk/v1/leads/export/YOUR_EXPORT_ID/enqueue.json`

#### III.Status of the job
`curl --header "Authorization: Bearer YOUR_ACCESS_TOKEN" https://123.mktorest.com//bulk/v1/leads/export/YOUR_EXPORT_ID/status.json`
    
#### IV. Download the file
`curl --header "Authorization: Bearer YOUR_ACCESS_TOKEN" https://123.mktorest.com//bulk/v1/leads/export/YOUR_EXPORT_ID/file.json`





## 2. Marketo Wrangler
Marketo Wrangler automates the bulk download process. 

### 2.1 Pre-requisite

* Install Python 2.7 

* Create `client_info.json` file with API credentials. File should have the following structure:  
```
{
	"client_id":"",
	"client_secret": "",
	"authorized_user": "",
    "marketo_instance": ""
}
```

### 2.2 List all server jobs and their status
#### `python marketo_wrangler.py --entity=lead --operation=list-jobs`

#### Response: 

`
{
    "requestId": "123abcde",
    "success": true,
    "result": [
        {
            "status": "Completed",
            "format": "CSV",
            "queuedAt": "2017-10-18T21:49:05Z",
            "numberOfRecords": 41175,
            "exportId": "cdefgh123456",
            "finishedAt": "2017-10-18T21:54:46Z",
            "fileSize": 43540685,
            "startedAt": "2017-10-18T21:51:50Z",
            "createdAt": "2017-10-18T21:49:04Z"
        },
       ..
     ]
}
`

### 2.3 Download Lead (Constituent) information over a time period

#### `python -u marketo_wrangler.py --entity=lead --operation=batch --startDate=<date string> --endDate=<date string>`

#### Description: 
Command will download leads whose records were created between the `startDate` and `endDate`. Format of the date is `YYYY-MM-DD` e.g., `2017-08-31`.

#### Response: 
* Script response will look like the following:

```
{'createdAt': {'startAt': '2017-06-16T00:00:00Z', 'endAt': '2017-7-15T00:00:00Z'}}   
Waiting for the job to be completed  
Job status Queued  

Waiting for the job to be completed  
Job status Queued  

Waiting for the job to be completed  
Job status Processing  

Job is completed. Downloading ....   
Download completed.   
```


#### `python -u marketo_wrangler.py --entity=lead --operation=batch --date=<date_string> --daysoffset=<number of days> --limit=<number of time periods>`

#### Description: 

Command will download leads whose records were created in the last few months ending on the `date_string` and progress backwards by days equal to `dayoffset`. Number of time periods is the value of `limit`. Given `date_string` is `2017-08-29`, `limit` is `3` and `daysoffset` is `29`, the method will create 3 jobs with the following time periods:  
                        `2017-08-01` to `2017-08-29`  
                        `2017-07-02` to `2017-07-31`  
                        `2017-06-02` to `2017-07-01`  

#### Response: 
* Script response will look like the following:

```
{'createdAt': {'startAt': '2017-06-16T00:00:00Z', 'endAt': '2017-7-15T00:00:00Z'}}   
Waiting for the job to be completed  
Job status Queued  

Waiting for the job to be completed  
Job status Processing  

Job is completed. Downloading ....   
Download completed.   

{'createdAt': {'startAt': '2017-05-18T00:00:00Z', 'endAt': '2017-06-16T00:00:00Z'}}  
Waiting for the job to be completed  
Job status Queued  

```

* If daily data transfer quota is exceeded, response will be the following:  
```
Exception: Job create request was not successful
```

* Leads information will be downloaded `job_output` folder as `<job_id>.csv`
* Job description (time period) will be stored in `job_description` folder as `<job_id>.json`

#### `python marketo_wrangler.py --entity=lead --operation=individual --leadID=1010101`
Command will display information of a given lead.

### 2.4 Download Activity information over a time period

#### `python -u marketo_wrangler.py --entity=activity --operation=batch --startDate=2017-10-23 --endDate=2017-10-25`
The above command will download all the activity information between the two dates. Set of activity type are limited to the following.

```
Click Email
Click Link
Email Bounced
Email Bounced Soft
Open Email
Send Email
Unsubscribe Email
Visit Webpage
Change Status in Progression
```
To update the list of activity types, edit the constant in `script\MarkActivity.py`


### 2.5 Download list of programs (Email and event)

#### `python -u marketo_wrangler.py --entity=program --operation=list-programs`
The above command will list emails, events and other program information. Output is also stored in `data\programs.csv`.  

## 3. Write Records to database
This module of the wrangler helps to write records from .csv files to database tables. 

### 3.1 Pre-requisite
Create `connection_config.json` file with server information. File should have the following structure:  
```
{
    "server": "",
    "input_database": "",
    "output_database": ""
}
```

Changes the following constants in `DBWriter.py`. This is required for the job output only.
```
TABLE_NAME=""
```

### 3.2 Write Job output to database

#### `python -u DBWriter.py --entity=activity --operation=load-job-output`

The above command will read files from directory `job_output\{ENTITY}`. Each file will processed by a Preprocessor class specific to entity type. After preprocessing, the records of the file will be written to the database `{TABLE_NAME}`. File name will be added to the list of processed files in `job_output_processed\{ENTITY}\processed_files.csv` 

### 3.3 Write Individual file to database 

#### `python -u DBWriter.py --entity=program --operation=load-file --filename='data\programs.csv' --tablename='Marketo Program'`
