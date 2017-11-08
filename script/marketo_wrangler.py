
# Marketo wrangler is the driver program to crawl lead (Constituent) information and Program (events and email) membershp

#import all the libraries
import os
import json
import pandas as pd
from datetime import datetime, timedelta
import time
import argparse

#change to the root director
os.chdir('..')
os.getcwd()


#import all the classes
from MarkCrawl import MarkCrawl
from MarkActivity import MarkActivity
from MarkJob import MarkJob
from Preprocess import Preprocess
from MarkProgram import MarkProgram
from MarkLead import MarkLead


#parse command line options
parser = argparse.ArgumentParser(description='Description of your program')
parser.add_argument('--entity', required=True)
parser.add_argument('--operation', required=True)
parser.add_argument('--date', required=False)
parser.add_argument('--limit', required=False)
parser.add_argument('--dayoffset', required=False)
 

parser.add_argument('--startDate', required=False)
parser.add_argument('--endDate', required=False)
parser.add_argument('--leadID', required=False)
 
args = vars(parser.parse_args())

#call routine based on command line options
if args['operation']=='batch' and args['date'] is not None:
    MarkJob(args['entity']).download_batch(args['date'],limit=int(args['limit']),dayoffset=int(args['dayoffset']))
    
elif args['operation']=='batch' and args['startDate'] is not None and args['endDate'] is not None:
    MarkJob(args['entity']).download_one_month(args['endDate'], args['startDate'])
 
elif args['operation']=='list-jobs':
    print json.dumps(MarkJob(args['entity']).list_server_jobs(), indent=4)

elif args['operation']=='list-programs':
    print json.dumps(MarkProgram().wrapper_list_programs(), indent=4)

elif args['entity']=='lead' and args['operation']=='individual' and args['leadID'] is not None:
    print json.dumps(MarkLead().get_lead_info(args['leadID']), indent=4)
 
