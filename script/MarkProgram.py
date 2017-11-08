import json
import os

from MarkCrawl import MarkCrawl
from Preprocess import Preprocess

class MarkProgram(MarkCrawl):
    '''Class containing utilities to extract Asset information (Events, Email) information 
    '''
    
    def __init__(self):
        super(MarkProgram, self).__init__('program')
        self.prep=Preprocess()
        self.request_body_filename='body.json'   #exception: file will created in the main dir
        self.PROGRAM_EP='/rest/asset/v1/program/{}.json'
        self.LEADS_BY_PROGRAM_EP='/rest/v1/leads/programs/{}.json'
        self.LIST_PROGRAMS_EP='/rest/asset/v1/programs.json?maxReturn=200'
        self.LIST_CAMPAIGNS_EP='/rest/v1/campaigns.json?batchSize=1000&workspaceName=Development'
 
    
    def get_list_programs_ep(self):
        # Return complete end point for list of programs
        return self.form_url(self.LIST_PROGRAMS_EP)

    def get_leads_by_program_ep(self):
        # Return complete end point for leads by program id
        return self.form_url(self.LEADS_BY_PROGRAM_EP)

    def get_program_ep(self):
        # Return complete program end point
        return self.form_url(self.PROGRAM_EP)

    def get_list_campaigns_ep(self):
        # Return a complete campaign end point 
        return self.form_url(self.LIST_CAMPAIGNS_EP)

    def list_campaigns(self):
        # Return api response data in json for the list of campaigns end point

        return self.prep_req_and_query(self.get_list_campaigns_ep())

    def get_asset_info(self, program_id):
        # Request and show the records of an asset
        url=self.get_program_ep().format(program_id)
        return self.prep_req_and_query(url)    
    
    def list_programs(self):
        # Return api response data in json for the list of program end point
        return self.prep_req_and_query(self.get_list_programs_ep())

    def process_and_store_json(self, json_data, ignore_keys, keys_to_flat, folder_and_filename):
        '''Flatten one of values in the json_data and store value of the key 'result' in a file
            Args:
                json_data: data in a key, value format
                ignore_keys: list of keys won't be included in the file
                keys_to_flat: list of keys whose value are in (key,value) format 
                                and will be flattened
                folder_and_filename: string containing os.path of the filename


        '''
        rs=json_data
        p_df=self.prep.flatten_list_of_dict(rs, ignore_keys=ignore_keys, keys_to_flat=keys_to_flat)
        p_df.to_csv(folder_and_filename,index=False)
 
    def wrapper_list_campaigns(self):
        ''' Call api for list of campaigns and store them in a flat csv file
            Returns:
                string representation of the json data for "result"
        '''
        rs=self.list_campaigns()
        rs=rs['result']
        self.process_and_store_json(rs, ignore_keys=[], keys_to_flat=[], folder_and_filename='data/campaigns.csv')
        return (json.dumps(rs, indent=4))

    def wrapper_list_programs(self):
        ''' Call api for list of programs and store them in a flat csv file
            Returns:
                string representation of the json data for "result"
        '''
        rs=self.list_programs()
        rs=rs['result']
        self.process_and_store_json(rs, ignore_keys=[], keys_to_flat=['folder'], folder_and_filename='data/programs.csv')
        return (json.dumps(rs, indent=4))

    def get_leads_by_program_id(self, program_id, suffix=''):
        # Request and show the leads who are member of a program
        url=self.get_leads_by_program_ep().format(str(program_id))
        return self.prep_req_and_query(url, suffix)
    
    def get_page_request(self, program_id, suffix=''): 
        # Interface method to call specific method such as get_leads_by_program_id 
        #return url end point used to generate page result
        return self.get_leads_by_program_id(program_id, suffix)
    
    def get_job_request(self, Filter):
        # Interface method is not yet implemented 
        pass
    

if __name__=='__main__':
    os.chdir('..')
    print MarkProgram().wrapper_list_campaigns()

