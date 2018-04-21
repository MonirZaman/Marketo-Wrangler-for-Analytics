import json
import os

from MarkCrawl import MarkCrawl
from Preprocess import Preprocess



class MarkActivity(MarkCrawl):
    '''Class containing utilities to extract Activity information 
    '''
    
    def __init__(self):
        super(MarkActivity, self).__init__('activity')
        self.prep=Preprocess()     #instantiate preprocessor
        self.GET_TYPE_EP='/rest/v1/activities/types.json'
        self.JOB_CREATE_EP='/bulk/v1/activities/export/create.json'
        self.JOB_PREFIX_EP='/bulk/v1/activities/export/'
        self.ALL_JOB_STATUS_EP='/bulk/v1/activities/export.json?status=Completed,Failed,Queued'
        self.request_body_filename='body.json'
        
        # We will retrieve a subset of activities
        self.ACTIVITY_TYPE_NAME='activityTypeIds'
        self.ACTIVITY_MAP={
                    'Click Email':11,
                    'Click Link':3,
                    'Email Bounced':8,
                    'Email Bounced Soft':27,
                    'Open Email':10,
                    'Send Email':6,
                    'Unsubscribe Email':9,
                    'Visit Webpage':1,
                    'Change Status in Progression':104
        }

        self.activity_types=self.ACTIVITY_MAP.values()
 
    def get_job_prefix_ep(self):
        # Concatenate and return instance url with job prefix end point
        return self.form_url(self.JOB_PREFIX_EP)
 
    def get_job_create_ep(self):
        # Concatenate and return instance url with job create end point
        return self.form_url(self.JOB_CREATE_EP)
 
    def get_all_job_status_ep(self):
        # Concatenate and return instance url with job status end point
        return self.form_url(self.ALL_JOB_STATUS_EP)

    def get_type_ep(self):
        # Concatenate and return instance url with activity type end point
        return self.form_url(self.GET_TYPE_EP)

    def get_activity_type(self):
        # Show the list of activity types
        url=self.GET_TYPE_EP
        return self.prep_req_and_query(url)
    
    def create_request_data(self, Filter):
        '''Create request json object, writes it as body.json and also returns it as dictionary
            Args:
                Filter: a dictionary typically containing start and end date.
            Returns:
                a dictionary.

            Example of argument Filter is the following,
            {
                'createdAt': {
                     'startAt': '2017-10-01T00:00:00Z',
                     'endAt': '2017-10-10T00:00:00Z'
                   }
            }

        '''
        
        rj={
            'filter': Filter
        }

        with open(self.request_body_filename,'w') as f:
            f.write(json.dumps(rj, indent=True))
        
        return rj
    
    def get_job_request(self, Filter):
        '''Prepare a complete curl request to create job 
            
            Args:
                Filter: a dictionary typically containing start and end date.
            Returns:
                curl request string and json request data as dictionary.
        
        '''
        Filter[self.ACTIVITY_TYPE_NAME]=self.activity_types
        
        #prepare request, write req as json file and also return req json 
        json_req_data=self.create_request_data(Filter)
        
        req=self.get_prefix_with_authorization()+' -X POST -H "Content-Type: application/json" -d @'\
                                                + self.request_body_filename\
                                                +' '+self.get_job_create_ep()
                
        return req, json_req_data

if __name__=='__main__':
    print MarkActivity().name
