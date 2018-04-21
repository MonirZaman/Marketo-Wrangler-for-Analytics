'''
        CLASS IMPLEMENTATION TO BE COMPLETED
'''
class MarkPaging(object):
    '''Class containing utilities to retrieve results with multiple pages:
        * Paging results - when result contains more than 300 items, 
                           we need to page through the results using next page token.
    '''
    def __init__(self, job_type):
        self.job_object=None
        self.token_key='nextPageToken'
        
        if job_type=='lead':
            self.job_object=MarkLead()
        elif job_type=='program':
            self.job_object=MarkProgram()
    
    def has_next_page(self, result):
        if result.has_key(self.token_key):
            if (result[self.token_key]!=None and result[self.token_key]!=''):
                return len(result['result'])>0
            else:
                return False
        
        else:
            return False
    
    def append_result(self, all_results, current_result):
        '''
            Args:
                all_results: dictionary object with keys 
                current_result: dictionary object
                
            Example format 
                {
                     "nextPageToken": "adbdd===", 
                     "requestId": "abc", 
                     "success": true, 
                     "result": [
                      {
                       "firstName": "iron", 
                       "lastName": "Man", 
                       "id": 29080, 
                       "membership": {
                            "reachedSuccess": false, 
                            "progressionStatus": "Invited", 
                            "isExhausted": false, 
                            "membershipDate": "2016-11-24T16:39:08Z", 
                            "acquiredBy": false
                       }, 
                       "updatedAt": "2017-08-14T23:05:15Z", 
                        "email": "iron.man@haskayne.ucalgary.ca", 
                       "createdAt": "2015-09-25T17:36:58Z"
                      }
                     ]
                    }
        '''
        all_results['result'].extend(current_result['result'])
        for akey in current_result.keys():  #keep the latest results for the rest of the keys
            if akey != 'result':
                all_results[akey]=current_result[akey]
        
        
    def create_checkpoint(self, result_dict):
        pass
    
    def paging(self, entity_id, limit):
        '''Request end point and retrieve data from all the pages
            Args:
                entity_id: identity of the object such as program id
            Returns:
                dictionary where result key will contain a list of results from 
                all the pages
        '''
        print 'Starting paging : '
        
        all_page_results={}
        
        result=self.job_object.get_page_request(self, entity_id)
        result=self.job_object.examine_status(result)
        self.append_result(all_page_results, result)
        
        i=0
        while self.has_next_page(result):
            print 'next page..'+'\n\n\n'
            
            suffix='?{}={}'.format(self.token_key, result[self.token_key])
            result=self.job_object.get_page_request(self, entity_id, suffix)
            result=self.job_object.examine_status(result)
            self.append_result(all_page_results, result)
        
            print (result)
            print (len(append_result['result']))
        
            i=i+1
            if i%10==0:
                self.create_checkpoint(all_page_result)
                
            if limit != -1:
                if i>=limit:
                    break
        



