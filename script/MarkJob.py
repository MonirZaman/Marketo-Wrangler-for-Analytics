import os
import json
import time
from datetime import datetime, timedelta

from MarkActivity import MarkActivity
from MarkLead import MarkLead

class MarkJob(object):
    '''Class containing utilities to create, start jobs. These jobs
         bulk extract (Lead and Activity information) from the Marketo Server
        
    '''
    
    def __init__(self, job_type):
        self.jo=None
        
        if job_type=='lead':
            self.jo=MarkLead()
        
        elif job_type=='activity':
            self.jo=MarkActivity()

        elif job_type=='program':
            self.jo=MarkProgram()
        
    def create_job(self, Filter):   
        '''Create a job on the server and return whether
            the create request was successful

            Args:
                Filter: dictionary containing field names and time period
            Return:
                dictionary containing status of the create request
        '''
        ep,json_req_data=self.jo.get_job_request(Filter)
        status=self.jo.query_endpoint(ep)
        return status
    
    
    def list_server_jobs(self):
        #list all the server jobs and show their status

        return self.jo.prep_req_and_query(self.jo.get_all_job_status_ep())
        
        
        
    def job_status(self, job_id):
        '''Return status of a job
            Args:
                job_id: job identifier string
            Returns:
                Response json containing the job's status
        '''
        
        #form url
        url=self.jo.get_job_prefix_ep()\
                +job_id\
                +'/status.json'
        
        return self.jo.prep_req_and_query(url)
    
    def start_job(self, job_id):
        '''Start a job
            Args:
                job_id: job identifier string
            Returns:
                Response json containing the start request
        '''
 
        #form POST request
        req=self.jo.get_prefix_with_authorization()+' '\
                +'-X POST '\
                +self.jo.get_job_prefix_ep()\
                +job_id\
                +'/enqueue.json'
    
        return self.jo.query_endpoint(req)
    
    def get_file(self, job_id):
        '''Download the result of a job as file 
            Args:
                job_id: job identifier string
            Returns:
                content of the file as plain text
        '''
 
        #form request
        url=self.jo.get_job_prefix_ep() +job_id + '/file.json'
        
        return self.jo.prep_req_and_query(url, json_encode=False)
    
    def create_filter(self, end_date_str, start_date_str=None):
        '''Return a dictionary with start_date_str and end_date_str
            Args:
                end_date_str: represents end date in the time period
                start_date_str: represents start date in the time period
            Returns:
                a dictionary containing time period
        '''
        
        if start_date_str is None:
            start_date_obj=datetime.strptime(end_date_str, '%Y-%m-%d')-timedelta(self.dayoffset)
            start_date_str=start_date_obj.strftime('%Y-%m-%d')
            
        return {'createdAt': { 'startAt': start_date_str+'T00:00:00Z', 'endAt': end_date_str+'T00:00:00Z' }}
 
    def download_one_month(self, end_date_str, start_date_str=None):
        '''Bulk extract information for one time period  
            Args:
                end_date_str: represents end date in the time period
                start_date_str: represents start date in the time period
            Returns:
                None. Information extracted are saved as file
        '''
        date_filter=self.create_filter(end_date_str, start_date_str)
        self.job_workflow(date_filter)
 

    def download(self, job_id):
        '''wrapper method to download a job's output as file
            Args:
                job_id: job identifier
            Returns:
                integer denoting the success of the task. Job output is stored as file when completed.
                0 denotes success and -1 denotes failure
                

        '''
        
        result=self.job_status(job_id)['result'][0]  # format of status {u'requestId': u'48', u'result': [{u'createdAt': u'2017-10-12T17:51:15Z',}]}
        if result.has_key('status'):
            if result['status']=='Completed':
                data=self.get_file(job_id)
                with open(os.path.join('job_output', self.jo.name, job_id+'.csv'), 'w') as f:
                    f.write(data)
                return (0)

            else:
                print 'Status of the job is ', result['status']
        else:
            print 'Status of the job could not be found'  

        return (-1)
    
    def job_workflow(self, Filter):
        '''Automate a job by creating, starting, downloading the output
            Args:
                Filter: dictionary containing time period
            Returns:
                None. Job output is stored as file.
        '''
        
        print ('Time period ', Filter)
        
        cstatus=self.create_job(Filter)
        
        # Create job
        cstatus=self.jo.examine_status(cstatus, 'job create')
        if cstatus['result'][0]['status']!='Created':
            raise Exception('Job creation was not successful')
        job_id= cstatus['result'][0]['exportId']   
        
       
        # Start job
        sstatus=self.jo.examine_status(self.start_job(job_id), 'job start')
                
        dstatus=self.jo.examine_status(self.job_status(job_id), 'job status')
        while (dstatus[u'result'][0][u'status']!='Completed'):
            #add a delay
            print 'Waiting for the job to be completed'
            print 'Job status '+ dstatus[u'result'][0][u'status']
            time.sleep(30)
            dstatus=self.jo.examine_status(self.job_status(job_id), 'job status')
        
        # Download job's output
        print 'Job is completed. Downloading ....'
        status = self.download(job_id)  # status 0 denote success

        if status == 0:
            print 'Download completed.'

            #write job description
            with open(os.path.join('job_description', self.jo.name, job_id+'.txt'),'w') as f:
                f.write(json.dumps(Filter, indent=4))
    
    def download_batch(self, end_date_str, limit = 5, dayoffset=29):
        ''' Create job over multiple time periods starting from end_date_str
                and progressing backwards by days equal to dayoffset. Number of 
                time periods is the value of limit.

                Given end_date_str is 2017-08-29 and limit is 3, the method will 
                create 3 jobs with the following time periods,
                        2017-08-01 to 2017-08-29
                        2017-07-02 to 2017-07-31
                        2017-06-02 to 2017-07-01

                Args:
                    end_date_str: representing end date over all the time periods
                    limit: number of time periods
                    dayoffset: duration of each time period in days
                
        '''

        for b in range(limit):
            start_date_obj=datetime.strptime(end_date_str, '%Y-%m-%d')-timedelta(dayoffset)
            start_date_str=start_date_obj.strftime('%Y-%m-%d')
        
            #create Filter
        
            Filter={'createdAt': { 'startAt': start_date_str+'T00:00:00Z', 'endAt': end_date_str+'T00:00:00Z' }}
            self.job_workflow(Filter)
        
            end_date_str=start_date_str
        #end of the for loop
    #end of method download_batch
