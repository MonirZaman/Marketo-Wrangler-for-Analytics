import os
import pandas as pd
import sys
import json

from Preprocess import Preprocess
from Preprocess import Preprocess_Activity

#Add library path to import DBWriter_worker
sys.path.append(os.path.abspath(""))
import read_from_database as rdb
from write_to_database import DBWriter_worker as dw

COLUMN_NAME_FILE='lead_column_names.csv'
TABLE_NAME='Private Marketo Lead Activity'

class DBWriter(object):
    '''Class contains routines to read records from csv files and 
        load into database
    '''
    def __init__(self, entity):
        self.config_filename='connection_config.json'
        self.entity=entity
        self.process_filename='processed_files.csv'

        # populate directory name containing data
        self.processed_folder=os.path.join('job_output_processed', entity)
        self.job_folder=os.path.join('job_output', entity)

        # instantiate entity specific preprocessor
        if entity == 'activity':
            self.preprocessor=Preprocess_Activity()
        else:
            self.preprocessor=Preprocess()
        self.w=dw(self.config_filename)  #writer object

    def read_file_names(self, folder_name):
        # Return file names stored in the folder as a list 

        for (dirpath, dirs, files) in os.walk(folder_name):
            break

        return files

    def read_and_write_afile(self, folder_and_filename, columns, writerObject, table_name, if_exists_value):
        '''Read, process data from csv file and store into database
            Args:
                folder_and_filename: string containing file name preceeded by location
                columns: list of column names. will be used in the future.
                writerObject: instance of DBWriter_worker
                table_name: name of the table in the database where records will be imported
                if_exists_value: string indicating whether data will be appended to the table
        '''

        print ('Processing file {}'.format(folder_and_filename))

        cdf=pd.read_csv(folder_and_filename, encoding='utf-8')
        cdf=self.preprocessor.process(cdf)
           
        writerObject.write_to_database(cdf, table_name, if_exists_value)
        print (cdf.head())


    def get_processed_files(self):
        #return files that have been processed so far as a dict where filename is the key e.g. {filename:True}
        f=os.path.join(self.processed_folder, self.process_filename)

        if os.path.exists(f):
            pdf=pd.read_csv(f)
            processed_files=pdf[pdf.columns.values[0]].map(lambda x: (x, True))
            processed_files=processed_files.values
            return dict(processed_files)
        else:
            return {}

    def append_processed_files(self, filename):
        #add filename to the list of processed files
        
        f=os.path.join(self.processed_folder, self.process_filename)
        if os.path.exists(f):
            with open(f, 'a') as fobj:
                fobj.write(filename+'\n')
        else:
            with open(f, 'w') as fobj:
                fobj.write('filename\n')
                fobj.write(filename+'\n')
    
    def load_files(self, files, columns, writerObject, table_name, if_exists_value='append'):
        '''Write CSV file content one at a time
            Args:
                dir: folder name
                files: list of files
                columns: list of columns
                writerObject: object of class DBWriter_worker
                table_name: name of the table where data will be written
                scrape_table: when true, scrape off the table for the first time


        '''
        
        #get a list of columns
        if len(files)<1:
            raise Exception('No files in the list')

        processed_files=self.get_processed_files()
        
        for afile in files:
            if afile not in processed_files:
                folder_and_filename=os.path.join(self.job_folder, afile)
                self.read_and_write_afile(folder_and_filename, columns, writerObject, table_name, 'append')         
                self.append_processed_files(afile)

    def combineDataset(self, dirname, files, columns):
        '''Read CSV file and concate it one dataset.
            columns is a list of names that we use to
            ensure proper ordering of columns
            
        '''
        #get a list of columns
        
        list_df=[]
        for afile in files:
            cdf=pd.read_csv(os.path.join(dirname, afile))
            cdf=cdf.drop_duplicates()
            cdf=cdf[columns]
            
            list_df.append(cdf)

        master_df=pd.concat(list_df)
        master_df=master_df.drop_duplicates()
        
        return master_df

    def write_column_names(self, dirname, afile):
        #write the column names of the csv file in another file; will write one time

        df=pd.read_csv(os.path.join(dirname, afile))
        column_df=pd.DataFrame({'column_names':df.columns.values})
        column_df.to_csv(COLUMN_NAME_FILE, index=False)

    def read_column_names(self):
        column_names=pd.read_csv(COLUMN_NAME_FILE)
        return column_names[column_names.columns[0]].values
    
    def write_job_output(self):
        # Write output of a job into database
        folder_name=os.path.join('job_output', self.entity)
        files=self.read_file_names(folder_name)
        columns=self.read_column_names()

        self.load_files(files, columns, self.w, TABLE_NAME, if_exists_value='append')  
        print (self.get_processed_files())

    def write_file_to_database(self, filename, tablename):
        '''Write the content of the file into a database table
            Args:
                filename: filename preceeded by folder name e.g., data\programs.csv
                tablename: string containing table name
        '''

        self.read_and_write_afile(filename, [], self.w, tablename, 'replace')   #[] as columns, 'replace' as if_exists_value


if __name__=='__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Description of your program')
    parser.add_argument('--entity', required=True)
    parser.add_argument('--operation', required=True)
    parser.add_argument('--filename', required=False)
    parser.add_argument('--tablename', required=False)
    
    args = vars(parser.parse_args())

    os.chdir('..')
    print (os.getcwd())

    if args['operation']=='load-job-output':
 
        c=DBWriter(args['entity'])
        c.write_job_output()

    elif args['operation']=='load-file':
        c=DBWriter(args['entity'])
        c.write_file_to_database(args['filename'], args['tablename'])
 


