import os
import time
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import datetime as dt
from dateutil.relativedelta import relativedelta
import argparse
from zuellig.helpers.utils import (
    logger_decorator,
    timer_decorator,
    cpu_mem_decorator)
import pytz
from dateutil.parser import parse
from zdt import country_map_master, time_func, log_func, hana_connector_utils, random_utils
import logging
import socket

class bq_connector():

    def __init__(self, country_list: str = 'Any'):

        self.country_list = country_list.split(',')

        # get configurations to query hana and upload to bq
         
        if self.country_list[0] != 'ID':       

            self.excel_config_path = os.getenv('bq_sql_config_path')

        elif self.country_list[0] == 'ID': 
            
            self.excel_config_path = os.getenv('bq_sql_config_path_hip')

        # get bq json key path
        
        self.bq_details_path = os.getenv('bq_details_path')

    def get_bq_config(self, view_name: str, write_options: list):

        # setting bq configs

        self.write_options = write_options

        excel_config_df_temp = pd.read_csv(self.excel_config_path,header='infer')

        self.excel_config_df = excel_config_df_temp[excel_config_df_temp['view_name']== view_name]

        self.bq_destination_table = self.excel_config_df['bq_destination_table'].iloc[0]

        self.partition_by_field = self.excel_config_df['partition_by_field'].iloc[0]

        self.cluster_by_field = self.excel_config_df['cluster_by_field'].iloc[0]

        self.delta = int(self.excel_config_df['delta'].iloc[0])

    @logger_decorator(filename=None)
    @cpu_mem_decorator
    @timer_decorator
    def upload_folder_to_bq(self, delta_unit, time_func_name, adhoc = None, country_list = []):

        '''
        This function is to load dataframe into a bq table that utilizes range partitioning
        '''

        try:

            folder = r'./pq_output/'

            # create file list to loop through

            file_list_temp = [f for f in os.listdir(folder)]

            #country_exclude_list =['MY']
            #file_list_temp = [f for f in file_list_temp if f[:2] not in country_exclude_list]

            #country_include_list =['MY']
            #file_list_temp = [f for f in file_list_temp if f[:2] in country_include_list]

            period_start, period_end = time_func.get_time_periods(delta = self.delta, delta_unit= delta_unit, time_func_name= time_func_name)

            period_start1 = period_start[0]

            file_list = [os.path.join(folder,f) for f in file_list_temp if int(f[8:16]) >= int(period_start1)]

            if adhoc == None:

                if args.halves:

                    folder = r'./pq_output_halves/'

                    file_list_temp = [f for f in os.listdir(folder)]

                    yesterday = (dt.datetime.now().date()-dt.timedelta(days=1))

                    self.period_start, self.period_end = time_func.get_time_periods(delta = self.delta, delta_unit= args.delta_unit, time_func_name= args.time_func_name, end_date = yesterday)
                        
                    self.period_start, self.period_end = time_func.split_period_list(self.period_start, self.period_end,args.halves)

                    period_start1 = period_start[0]

                    file_list_halves = [os.path.join(folder,f) for f in file_list_temp if int(f[8:16]) >= int(period_start1)]

                    file_list+= file_list_halves

            if adhoc == 'yes':
                
                folder = r'./pq_output_adhoc/'

                yesterday1 = (dt.datetime.now().date()-dt.timedelta(1)).strftime('%Y%m%d')

                file_list_temp = [f for f in os.listdir(folder)]

                file_list_temp1 = [f for f in file_list_temp if f[:2] in country_list]

                file_list = [os.path.join(folder,f) for f in file_list_temp1 if int(f[8:16]) >= int(yesterday1)]

            # write truncate first file first to bq table

            if file_list:

                with open(file_list[0], "rb") as source_file:

                    bq_client = bigquery.Client(credentials=service_account.Credentials.from_service_account_file(self.bq_details_path))
                    
                    job_config = bigquery.LoadJobConfig(
                        range_partitioning = bigquery.RangePartitioning(
                        field = self.partition_by_field,
                        range_ = bigquery.PartitionRange(start=1,end=21,interval=1)
                        ),
                        clustering_fields= self.cluster_by_field.split(','),
                        write_disposition= self.write_options[0],
                        source_format=bigquery.SourceFormat.PARQUET
                    )

                    job1 = bq_client.load_table_from_file(
                        source_file,
                        destination = self.bq_destination_table,
                        job_config=job_config
                        )
                
                job1.result()

                # append the rest

                for f in file_list[1:]:
                    
                    with open(f, "rb") as source_file:

                        bq_client = bigquery.Client(credentials=service_account.Credentials.from_service_account_file(self.bq_details_path))
                        
                        job_config = bigquery.LoadJobConfig(
                            range_partitioning = bigquery.RangePartitioning(
                            field = self.partition_by_field,
                            range_ = bigquery.PartitionRange(start=1,end=21,interval=1)
                            ),
                            clustering_fields= self.cluster_by_field.split(','),
                            write_disposition= self.write_options[1],
                            source_format=bigquery.SourceFormat.PARQUET
                        )

                        job1 = bq_client.load_table_from_file(
                            source_file,
                            destination = self.bq_destination_table,
                            job_config=job_config
                            )
                
                job1.result()
            
            else:

                print(f'\nNOTE: No file to upload {dt.datetime.now().time().replace( microsecond=0)}\n')

            print(f'\nNOTE: bq upload done {dt.datetime.now().time().replace( microsecond=0)}\n')

        except Exception:
            
            raise

    @logger_decorator(filename=None)
    @cpu_mem_decorator
    @timer_decorator
    def check_last_upload_query(self,ctry_id):

        ''' This query is if you wish to query bq and see what is the last value/date'''
    
        check_last_upload_query_sql_str = f"SELECT MAX(upload_datetime) FROM `{self.bq_destination_table}` WHERE country_id = {ctry_id}"
        
        bq_client = bigquery.Client(credentials=service_account.Credentials.from_service_account_file(self.bq_details_path))
        
        check_last_upload_job = bq_client.query(check_last_upload_query_sql_str)
        
        check_last_upload_job_df = check_last_upload_job.result().to_dataframe()
        
        last_upload_dt = check_last_upload_job_df.values[0][0]
        
        return last_upload_dt

if __name__ == '__main__':

    # load data from .env file
    load_dotenv()

    email_receivers1 = os.getenv('email_receivers_list1').split(',')

    email_receivers2 = os.getenv('email_receivers_list2').split(',')
    
    parser = argparse.ArgumentParser()

    parser.add_argument("--view_name", help="view_name", type=str)

    parser.add_argument("--write_options", help="write_options", type=str)

    parser.add_argument("--time_func_name", help="time_func_name", type=str)

    parser.add_argument("--delta_unit", help="delta_unit", type=str)

    parser.add_argument("--halves", help="delta_unit", type=int)

    # create args object to process the args inputs
    
    args = parser.parse_args()

    args.write_options = args.write_options.split(',')
    
    # log for processes in script

    log1 = log_func.log_it("ctry_logs")

    #log for entire process

    log2 =  log_func.log_it("process_log")
    
    # activate environment variables
    
    try:

        start_time = time.time() 

        log1.info(f'bq upload {__name__} started')

        log2.info(f'bq upload {__name__} started')
    
        print(f'\nNOTE: bq upload {__name__} started {dt.datetime.now().replace( microsecond=0)}\n')

        # instantiate bq obj and get paths

        bq_connector1 = bq_connector()

        # get config from excel

        bq_connector1.get_bq_config(args.view_name, args.write_options)

        # upload to bq

        bq_connector1.upload_folder_to_bq(delta_unit = args.delta_unit, time_func_name= args.time_func_name,)

        #print(f'3 bq upload succeeded {dt.datetime.now().replace( microsecond=0)}')

        end_time = time.time()

        run_time = end_time - start_time

        log1.info(f'3 bq_upload {__name__} completed successfully, run_time: {time_func.get_time_hh_mm_ss(run_time)}')

        log2.info(f'3 bq_upload {__name__} completed successfully, run_time: {time_func.get_time_hh_mm_ss(run_time)}')

    except Exception:

        random_utils.Emailer().send_it(receivers = email_receivers2, subject = f'BQ Upload exit - {args.view_name}' , content = f'hostname: {socket.gethostname()} <br />')

        log1.exception(f'3 bq_upload {__name__} fail')
    
        log2.exception(f'3 bq_upload {__name__} fail')

        time.sleep(5)
    
        raise