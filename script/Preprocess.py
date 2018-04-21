import json
import os
import pandas as pd

class Preprocess(object):
    '''Utility class to flatten json data
    '''
    
    def __init__(self):
        self.name='Utility class'
    
    def flatten_list_of_dict(self, ldict, **args):
        new_ldict=[]
        for adict in ldict:
            flat_dict=self.flatten_dict(adict, **args)
            new_ldict.append(flat_dict)
        
        
        return pd.DataFrame.from_records(new_ldict)
        
    def flatten_dict(self, adict, ignore_keys, keys_to_flat):
        '''flatten a dictionary and return the flat dictionary
            Args:
                adict: dictionary
                ignore_keys: keys that will not be included in the output dictionary
                keys_to_flat: keys whose values are dictionary
        '''
        #print 'dict show', adict 
        new_dict={}
        for akey in adict.keys():
            if akey not in set(ignore_keys) and akey not in set(keys_to_flat):
                new_dict[akey]=adict[akey]
        
        for akey in keys_to_flat:
            
            if akey not in adict:
                continue
            
            inner_dict=adict[akey]
            
            for bkey in inner_dict.keys():
                new_dict[bkey]=inner_dict[bkey]
        
        return new_dict



    def process(self, df, columns=[]):
        df=df.drop_duplicates()
        if len(columns)>0:
            df=df[columns]
 
        return df


class Preprocess_Activity(Preprocess):
    '''Class contains routines to preprocess activity related files
    '''
    def __init__(self):
        super(Preprocess_Activity, self).__init__()
        self.activity_map={1:'Visit Webpage', 2:'Fill Out Form', 3:'Click Link',\
                           6:'Send Email', 7:'Email Delivered', 8:'Email Bounced',\
                           9:'Unsubscribe Email', 10:'Open Email', 11:'Click Email',\
                           27:'Email Bounced Soft', 46:'Interesting Moment',\
                           104:'Change Status in Progression'}

        #column attributes is a dictionary and following file contains all the keys in the dictionary
        self.attribute_key_filename='attribute_keys.csv'
    
    def load_attribute_keys(self):
        return pd.read_csv(os.path.join('data', self.attribute_key_filename))['attribute_keys'].unique()
        

    def map_activity_name(self, df, colname,new_colname, map_dict):

        df[new_colname]=df[colname].apply(lambda x: map_dict[x])  
        return df

    def flatten_attribute(self, df, attribute_name):
        #attribute is a dictionary
        keys = self.load_attribute_keys()
 
        for key in keys:
            df[key]=df[attribute_name].apply(lambda x: (json.loads(x))[key] if key in (json.loads(x)) else None)      

        return df

    def process(self, df, columns=[]):
        df=super(Preprocess_Activity, self).process(df, columns)
        df=self.map_activity_name(df, 'activityTypeId', 'activityTypeName', self.activity_map)
        df=self.flatten_attribute(df,'attributes') 
        return df.drop(['attributes'], axis=1)

    def get_distinct_attribute_keys(self):
        # return list of keys for the dictionary attributes and also, write list to the attribute_keys.csv file 
        
        files=['job_output/activity/1.csv','job_output/activity/2.csv']

        keys_seen=dict()
        list_keys=[]

        for afile in files:
            df=pd.read_csv(afile)

            for i in range(df.shape[0]):
                av=json.loads(df.iloc[i]['attributes'])
                ckeys=','.join(sorted(list(av.keys())))
    
                if not keys_seen.has_key(ckeys):
                    keys_seen[ckeys]=True
                    list_keys.extend(av.keys())
        print (keys_seen.keys())
        pd.DataFrame({'attribute_keys':list(set(list_keys))}).to_csv('data/attribute_keys.csv',index=False)


if __name__=='__main__':
    os.chdir('..')
    Preprocess_Activity().get_distinct_attribute_keys()
