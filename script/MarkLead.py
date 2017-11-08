import os
import pandas as pd

from MarkCrawl import MarkCrawl
from Preprocess import Preprocess

class MarkLead(MarkCrawl):
    '''Class containing utilities to extract lead (Constituent) information 
    '''
    
    def __init__(self):
        super(MarkLead, self).__init__('lead')
        self.prep=Preprocess()
        self.lead_attribute_file_read='lead_attributes_filtered.csv'
        self.lead_attribute_file_write='lead_attributes.csv'
        self.request_body_filename='body.json'   #file will created in the root dir
        self.job_create_ep='/bulk/v1/leads/export/create.json'
        self.job_prefix_ep='/bulk/v1/leads/export/'
        self.all_job_status_ep='/bulk/v1/leads/export.json?status=Completed,Failed,Queued'
        self.lead_ep='/rest/v1/lead/{}.json?fields=id,email,firstName,lastName,mktoName,annualRevenue'
 
    def get_job_create_ep(self):
        #Concat instance url with job create end point and return the complete end point
        return self.form_url(self.job_create_ep)

    def get_job_prefix_ep(self):
        #Concat instance url with job prefix end point and return the complete end point
        return self.form_url(self.job_prefix_ep)

    def get_lead_ep(self):
        #Concat instance url with lead end point and return the complete end point
        return self.form_url(self.lead_ep)


    def get_all_job_status_ep(self):
        #Concat instance url with all_job_status end point and return the complete end point
        return self.form_url(self.all_job_status_ep)


    def get_lead_info(self, lead_id):
        #Request and show the records of a lead
        end_point=self.get_lead_ep()
        return self.prep_req_and_query(end_point.format(lead_id))
    
    
    def get_lead_attributes(self):
        #Read the list of available lead attributes and write to file

        la=self.get_lead_fields()
        adf=self.prep.flatten_list_of_dict(la, ignore_keys=['soap'], keys_to_flat=['rest'])
        adf.to_csv('data\\lead_attributes.csv', index=False)
    
    def read_lead_attributes(self):
        '''Read lead attributes from files
            Returns:
                a dictionary containing attributes, display names and column headers
        '''

        la_df=pd.read_csv(os.path.join('data',self.lead_attribute_file_read))
        
        name=la_df['name'].tolist()
        displayName=la_df['displayName'].tolist()
        columnHeaderNames=dict(zip(name, displayName))
        
        assert len(name)==len(columnHeaderNames.keys())
               
        return {'name': name, 'displayName':displayName, 'columnHeaderNames':columnHeaderNames}
    
    
    def create_request_data(self, name, columnHeaderNames, Filter):
        '''Create request json, writes it as body.json and also returns it as dictionary
            Returns:
                a dictionary containing request body
        '''
        
        rj={
            'fields':name, 
            'columnHeaderNames': columnHeaderNames,
            'format': 'CSV',
            'filter': Filter
        }


        
        with open(self.request_body_filename,'w') as f:
            f.write(json.dumps(rj, indent=True))
        
        return rj
    
    def get_job_request(self, Filter):
        '''Prepare a curl request to create job to extract lead information
            
            Args: 
                Filter: dictionary containing filter information e.g., start and end date
            Returns:
                curl request string and request body as dictionary
    
            Example format for Filter,
            {
                'createdAt': {
                     'startAt': '2017-10-01T00:00:00Z',
                     'endAt': '2017-10-10T00:00:00Z'
                   }
            }
        '''
        
        at=self.read_lead_attributes()
        
        name=at['name'] 
        displayName=at['displayName']
        columnHeaderNames=at['columnHeaderNames']
        assert len(name)==len(columnHeaderNames.keys())
        
        json_req_data=self.create_request_data(name, columnHeaderNames, Filter)
        
        req=self.get_prefix_with_authorization()+' -X POST -H "Content-Type: application/json" -d @'\
                +self.request_body_filename\
                +' '+self.get_job_create_ep()
                
        return req, json_req_data


if __name__=='__main__':
    print (MarkLead().get_lead_info(29101))
