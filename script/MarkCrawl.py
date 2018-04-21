import os
import json
import pandas as pd
from datetime import datetime, timedelta
import time


class MarkCrawl(object):
    '''Class responsible for getting access token and sending request
    '''
    def __init__(self, name=''):

        self.INSTANCE_URL=''
        self.REQ_PREFIX='curl --header "Authorization: Bearer {}"'
        self.IDENTITY_EP= '/identity/oauth/token'
        self.LEAD_EP='/rest/v1/lead/{}.json?fields=id,email,firstName,lastName,mktoName,annualRevenue'
        self.LEAD_FIELDS_EP='/rest/v1/leads/describe.json'
        self.client_info_file='client_info.json'
        self.name=name

    def read_client_info(self):
        # Read and return configuration information stored in json file
        with open(self.client_info_file) as f:
            return json.load(f)
    
    
    def prep_req(self, url, suffix=''):
        '''Prepare complete curl request (GET) based on arguments
            Args:
                url: endpoint. Example, https://1234567.mktorest.com/rest/v1/lead/2086.json
                suffix: query with question mark. Example,
                    * ?fields=id,email,firstName
                    * ?nextPageToken=34f44665653345
            Returns:
                curl request string. Example,
                
                curl --header "Authorization: Bearer <access_token>" https://*.mktorest.com/../2086.json?nextPageToken=34
        '''
        req=self.get_prefix_with_authorization()
        req=req+' '+url+suffix
        return req
    
    def prep_req_and_query(self, url, json_encode=True, suffix=''): 
        '''Prepare complete curl request (GET) based on arguments,
            and send request and return result.
            Args:
                url: endpoint. Example, https://1234567.mktorest.com/rest/v1/lead/2086.json
                suffix: query with question mark
                    * ?fields=id,email,firstName
                    * ?nextPageToken=34f44665653345
            Returns:
                json response object
        '''
        req=self.prep_req(url, suffix)
        return self.query_endpoint(req, json_encode)

    
    def query_endpoint(self, endpoint, json_decode=True):
        '''Send request to the endpoint and return the response
            Args:
                endpoint: curl command
                json_decode: response is converted to json when true
            
            Returns:
                response string from the server
                    
        '''
        content=os.popen(endpoint).read()
        if json_decode==True:
            #content = json.dumps(json.loads(content),indent=4)
            content = json.loads(content)
        return content


    def form_url(self, end_point):
        '''Join instance url with end_point
            Args:
                end_point: API end point e.g., /rest/programs.json
            Return:
                Complete end point prefixed by https://abc.marketo.com

        '''
        if self.INSTANCE_URL == '':
            raise Exception('Instance URL is not populated')

        return self.INSTANCE_URL+end_point

    def get_access_token(self):
        '''Request and return access token
            Returns:
                access token 
        '''

        info=self.read_client_info()

        req='curl -X POST -d "client_id={}&client_secret={}&grant_type=client_credentials&username={}" {}'\
                .format(info['client_id'],info['client_secret'],info['authorized_user'], self.form_url(self.IDENTITY_EP))
        
        s=os.popen(req).read()
        js=json.loads(s)
        return js['access_token']
    
    def get_sample(self):
        #Request and show a sample record

        at=self.get_access_token()
        req=self.REQ_PREFIX.format(at)
        req=req+' '+self.form_url(self.LEAD_EP).format(29086)
        return self.query_endpoint(req)
    
    def get_prefix_with_authorization(self):
        '''Request and prepare curl request prefix with 
            authorization token

            Returns:
                curl request prefix e.g., curl --header "Authorization: Bear YOUR_AUTH_TOKEN"
        '''

        at=self.get_access_token()
        req=self.REQ_PREFIX.format(at)
        return req
    
    def get_lead_fields(self):
        '''Return the list of lead attribute as a diction
            Returns: 
                A list of dictionary. 

            Example format,
            "result": [
                  {
                   "displayName": "Company Name", 
                   "dataType": "string", 
                   "rest": {
                        "readOnly": false, 
                        "name": "company"
                   }, 
                   "soap": {
                        "readOnly": false, 
                        "name": "Company"
                   }, 
               "length": 255, 
               "id": 2
            },
            ]
        '''
        pa=self.get_prefix_with_authorization()
        req=pa+' '+self.form_url(self.LEAD_FIELDS_EP)
        fields=self.query_endpoint(req)
        return fields['result']
    
    def examine_status(self, status, type_of_request):
        '''Check status and raise exception when error occurs
            Args:
                status: dictionary
                type_of_request: string describing request


                Sample Status with success,
                {u'requestId': u'58', u'success': True, 
                 u'result': [{u'status': u'Queued', u'exportId': u'12443343adfd', 
                                u'queuedAt': u'2017-10-13T17:12:46Z', u'createdAt': u'2017-10-13T17:12:45Z', u'format': u'CSV'}]
                }
                
                Sample Status with error
                {u'errors': [{u'message': u"Export 'jk4219-b394' is in Queued status", u'code': u'1029'}], 
                 u'requestId': u'abcdefas', u'success': False
                 }
        '''

        if status.has_key('errors') or status['success']==False or len(status[u'result'])<1:
            print status   
            raise Exception(type_of_request+' request was not successful')
            
        
        return status


