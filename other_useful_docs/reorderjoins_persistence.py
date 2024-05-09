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
from zdt import country_map_master,time_func, log_func, hana_connector_utils, random_utils
import random
import importlib
import sys
import socket

class hana_to_pq_bq(country_map_master.country_map_class):

    def __init__(self,country_list,*args, **kwargs):

        '''
        Environment variables are activated
        '''

        super(hana_to_pq_bq, self).__init__(*args, **kwargs)

        #input from argparse e.g. ph,my,sg
        self.country_list = country_list.split(',')

        # get configurations to query hana and then upload to bq
        if self.country_list[0] != 'ID':

            self.excel_config_path = os.getenv('bq_sql_config_path')

        elif self.country_list[0] == 'ID':
            
            self.excel_config_path = os.getenv('bq_sql_config_path_hip')

        # initiate hana connection

        self.hana_password_file = os.getenv('hana_details_path')

        # creating bq api object from json credentials where path is stored in env file
        
        self.bq_details_path = os.getenv('bq_details_path')

    def get_sql_config(self, view_name):

        ''' get configuration details as a df and extract sql statement info'''

        excel_config_df_temp = pd.read_csv(self.excel_config_path,header='infer')

        # get only the required config for that view

        self.excel_config_df = excel_config_df_temp[excel_config_df_temp['view_name']== view_name]

        self.view_name = view_name

        self.sql_str_unformatted = self.excel_config_df['sql_statement'].iloc[0]

        # get full columns of view
        # This is to add in the columns that some countries are missing later on in the script

        self.input_columns = None

        if str(self.excel_config_df['input_columns'].iloc[0])!='nan':

            columns_temp = self.excel_config_df['input_columns'].iloc[0].replace("\n",'').replace("'","").split(",")

            self.input_columns = [x.strip().lower() for x in columns_temp]

        # delta is how many months or years that we want to backtrack.

        self.delta = int(self.excel_config_df['delta'].iloc[0])

    @logger_decorator(filename=None)
    @cpu_mem_decorator
    @timer_decorator
    def query_hana_to_pq(self, view_name, sales_org_filter = [], suffix = None):

        '''

        sales_org_filter is the salesorg that you want. for example, country could be ph but you only want mdi
        
        This is the main function to query hana and upload data

        '''

        for ctry in self.country_list:

            log1.info(f"RS: {run_started} 0 Country started, country: {ctry}, sales_org_filter: {sales_org_filter}, suffix: {suffix}")

            #log2.info(f"RS: {run_started} 0 Country started, country: {ctry}, sales_org_filter: {sales_org_filter}, suffix: {suffix}")

            query_output = []
            
            # we loop thru sales_org so that we can filter out begrus that we dont want
            for tup in self.country_map[ctry]['begru_salesorg']:

                sales_org = tup[1]

                # here we want to split the first batch of time period in one vm and the second bath in another
                # do note that all time_splits must have sales_org_filter. 
                ## sales_org in sales_org_filter is added because there might be 2 country bat files and we only one to split one sales_org in 1 of the ctry bat files.
                ## if we dont add this, when running over all the salesorg in the country, it might just split the time period

                sales_org_time_split_list = [2500,1900,3102,3105]

                if sales_org in sales_org_time_split_list and sales_org in sales_org_filter and suffix==None:

                    yesterday = (dt.datetime.now().date()-dt.timedelta(days=1))

                    # generate time periods list

                    self.period_start, self.period_end = time_func.get_time_periods(delta = self.delta, delta_unit= args.delta_unit, time_func_name= args.time_func_name, end_date = yesterday)
                    
                    # split the time periods

                    self.period_start, self.period_end = time_func.split_period_list(self.period_start, self.period_end,args.halves)

                self.period_start.sort(reverse=True)
                
                self.period_end.sort(reverse=True)

                # loop thru time periods
                for index, period in enumerate(self.period_start):

                    date_from = period
                    DATETIME = self.period_end[index]
                    date_to = self.period_end[index]
                    begru = tup[0]
                    sales_org = tup[1]
                    plant = self.country_map[ctry]['plant']
                    currency = self.country_map[ctry]['currency']
                    country = ctry

                    for try_num in range(6):

                        sleep_duration = random.randint(5,10)

                        try:

                            # if user indicated sales_org, then we will only filter out matched sales_org
                            if sales_org_filter:

                                if sales_org in sales_org_filter:

                                    print(f'\nNOTE: Hana query started, country: {country}, sales_org: {sales_org}, time_period: {date_from} to {DATETIME}, suffix: {suffix}, time: {dt.datetime.now().time().replace( microsecond=0)}\n')

                                    # get hana connection and format sql string from excel

                                    if sales_org != 1900:
                                        self.hana_connector = hana_connector_utils.instantiate_hana_conn_bdp(self.hana_password_file)

                                    elif sales_org == 1900:
                                        self.hana_connector = hana_connector_utils.instantiate_hana_conn_hip(self.hana_password_file)

                                    #if in a function can use this: str2 = str1.format(**kwargs)
                                    self.sql_str = self.sql_str_unformatted.format(view_name = self.view_name, begru = begru, date_from = date_from,date_to=date_to, DATETIME = DATETIME, sales_org = sales_org, country = country, plant=plant, currency=currency)

                                    # query hana and change all columns to lower

                                    output_df = pd.read_sql(sql=self.sql_str, con=self.hana_connector)
                                    output_df.columns = output_df.columns.str.lower()

                                    # if dataframe is not empty, check if all columns are present

                                    if output_df.empty == False:
                                        
                                        if self.input_columns: 

                                            for col in self.input_columns:
                                                if col not in [str.lower(x).strip() for x in output_df.columns]:
                                            
                                                    output_df[col] = ' '

                                        query_output.append(output_df)

                                    else:

                                        log1.info(f'RS: {run_started} 1.5 empty df, country: {country},  begru: {begru}, sales_org: {sales_org}, suffix: {suffix}, time_period: {date_from} to {date_to}')

                                    log1.info(f'RS: {run_started} 1 Hana query, country: {country},  begru: {begru}, sales_org: {sales_org}, suffix: {suffix}, time_period: {date_from} to {date_to} succeed')

                            # if no begru filter then we just run the connection for that tuple
                            else:

                                if country!= 'ID':

                                    print(f'\nNOTE: Hana query started, country: {country}, sales_org: {sales_org}, time_period: {date_from} to {DATETIME}, suffix: {suffix}, time: {dt.datetime.now().time().replace( microsecond=0)}\n')

                                    self.hana_connector = hana_connector_utils.instantiate_hana_conn_bdp(self.hana_password_file)

                                    self.sql_str = self.sql_str_unformatted.format(view_name = self.view_name, begru = begru, date_from = date_from, date_to=date_to, DATETIME = DATETIME,sales_org = sales_org, country = country, plant=plant, currency=currency)

                                    output_df = pd.read_sql(sql=self.sql_str, con=self.hana_connector)
                                    
                                    output_df.columns = output_df.columns.str.lower()

                                    if output_df.empty == False:
                                        
                                        if self.input_columns: 

                                            for col in self.input_columns:
                                                if col not in [str.lower(x).strip() for x in output_df.columns]:
                                        
                                                    output_df[col] = ' '

                                        query_output.append(output_df)

                                    else:
                                        
                                        log1.info(f'RS: {run_started} 1.5 empty df, country: {country},  begru: {begru}, sales_org: {sales_org}, suffix: {suffix}, time_period: {date_from} to {date_to}')

                                    log1.info(f'RS: {run_started} 1 Hana query, country: {country},  begru: {begru}, sales_org: {sales_org}, suffix: {suffix}, time_period: {date_from} to {date_to} succeed')

                                elif country== 'ID':

                                    print(f'\nNOTE: Hana query started, country: {country}, sales_org: {sales_org}, time_period: {date_from} to {DATETIME}, suffix: {suffix}, time: {dt.datetime.now().time().replace( microsecond=0)}\n')

                                    self.hana_connector = hana_connector_utils.instantiate_hana_conn_hip(self.hana_password_file)

                                    self.sql_str = self.sql_str_unformatted.format(view_name = self.view_name, begru = begru, date_from = date_from, date_to=date_to, DATETIME = DATETIME,sales_org = sales_org, country = country, plant=plant, currency=currency)

                                    output_df = pd.read_sql(sql=self.sql_str, con=self.hana_connector)
                                    
                                    output_df.columns = output_df.columns.str.lower()

                                    if output_df.empty == False:

                                        if self.input_columns:

                                            for col in self.input_columns:
                                                if col not in [str.lower(x).strip() for x in output_df.columns]:
                                        
                                                    output_df[col] = ' '

                                        query_output.append(output_df)

                                    else:
                                            
                                        log1.info(f'RS: {run_started} 1.5 empty df, country: {country},  begru: {begru}, sales_org: {sales_org}, suffix: {suffix}, time_period: {date_from} to {date_to}')
                            
                                    log1.info(f'RS: {run_started} 1 Hana query, country: {country},  begru: {begru}, sales_org: {sales_org}, suffix: {suffix}, time_period: {date_from} to {date_to} succeed')

                            if query_output:         

                                # if there is a saved df instead of empty df            

                                final_df = pd.concat(query_output)

                                # change all fields to str or pq might not be able to save or bq might have issues when auto detecting

                                final_df = final_df.astype(str)

                                final_df['country'] = ctry

                                if 'datetime' in final_df.columns:
                                    
                                    # format column so that we can change it to dt

                                    final_df['datetime'] = final_df['datetime'].replace('None', '99991231000000').replace(' ', '99991231000000').apply(parse)

                                final_df['country_id'] = self.country_map[ctry]['country_id']

                                # changing sales_org for mdi and ispi
                                if sales_org == 2501:
                                    final_df['country_id'] = 13

                                if sales_org == 2504:
                                    final_df['country_id'] = 14

                                # unique row id we put in bq

                                final_df['bq_table_row_id'] = [ctry + str(sales_org) + '_' + date_from +  '_' + date_to + '_'+  str(i) for i in range(len(final_df))]
                                
                                # upload time in sgt
                                
                                final_df['save_datetime'] = dt.datetime.now(pytz.timezone('Asia/Singapore')).replace(microsecond=0)
                                
                                final_df['save_date'] = dt.datetime.now().date()
                                
                                final_df['save_time'] = dt.datetime.now().time().replace( microsecond=0)

                                # save to output folder by sales_org

                                pq_path = f"./pq_output/{ctry}_{sales_org}_{date_from}.parquet"

                                # this is for flexibility of saving for other non standard requests like only yesterday file
                                
                                if suffix:
                                    
                                    pq_path = f"./pq_output_adhoc/{ctry}_{sales_org}_{date_from}_{suffix}.parquet"

                                if suffix == None and args.halves:
                                    
                                    pq_path = f"./pq_output_halves/{ctry}_{sales_org}_{date_from}.parquet"

                                final_df = final_df.to_parquet(pq_path, index=False)

                                query_output = []
                                
                                log1.info(f'RS: {run_started} 2 Save to pq, country: {country}, begru: {begru}, sales_org: {sales_org}, suffix: {suffix}, time_period: {date_from} to {date_to} succeed')
                            
                            break

                        except Exception as exp:
                                
                            log1.exception(f'RS: {run_started} Hana Query and save, country: {country},  begru: {begru}, sales_org: {sales_org}, suffix: {suffix}, time_period: {date_from} to {date_to} failed on try {try_num} \n>{exp}')

                            log2.info(f'RS: {run_started} Hana Query and save, country: {country},  begru: {begru}, sales_org: {sales_org}, suffix: {suffix}, time_period: {date_from} to {date_to} failed on try {try_num}')

                            time.sleep(sleep_duration)

                        if try_num == 5:

                            log1.info(f'Hana exit, country: {country},  begru: {begru}, sales_org: {sales_org}, suffix: {suffix}, time_period: {date_from} to {date_to}, try_num: {try_num}')

                            log2.info(f'Hana exit, country: {country},  begru: {begru}, sales_org: {sales_org}, suffix: {suffix}, time_period: {date_from} to {date_to}, try_num: {try_num}')

                            random_utils.Emailer().send_it(receivers = email_receivers2, subject = f'Hana exit - {args.view_name} - {args.countries}' , content = f'hostname: {socket.gethostname()} <br /> begru: {begru} <br /> sales_org: {sales_org} <br /> suffix: {suffix} <br /> time_period: {date_from} to {date_to} <br /> try_num: {try_num} <br />')

                            sys.exit()
                            
    @staticmethod
    def check_hana_csm(hana_connector):

        today = dt.datetime.now().date()

        today_day, today_month, today_year  = today.day, today.month, today.year

        today_str = f'{today_month}/{today_day}/{today_year}'

        sql_str1 = '''SELECT DISTINCT ("UploadDate")
        FROM
        (
        select distinct( "UploadDate")
        from "CSM"."ZiP_Integration_Transaction_P_Synapse_Delete"
        UNION
        select distinct "UploadDate"
        from "CSM"."ZiP_Integration_Transaction_P_Synapse_Insert"
        UNION
        select distinct "UploadDate"
        from "CSM"."ZiP_Integration_Transaction_P_Synapse_Modify"
        )'''

        sql_str2 = f''' select MAX("UploadDate")
    from "CSM"."ZiP_Integration_Transaction_P_Synapse" where "UploadDate" like '%{today_str}%%';'''
        
        country_code = args.countries

        if country_code in ['MO','ID']:
            country_code = 'MY'

        if country_code=='PH':
            country_code = 'MDI'

        if country_code=='KR':
            country_code = 'KDS'

        if country_code == 'TH':

            sql_str_th = f'''
                Select
                        "PROCESS" AS "SCRIPT",
                        "STATUS","START"
                        from "CSM"."PROCESS_EXECUTION_LOGS_SYNAPSE"
                        where
                            "START" >= TO_VARCHAR(NOW(),'YYYY-MM-DD')
                            and "PROCESS" in
                            (
                '[Daily] Step 3 - ZiP_Integration_Transaction_P_{country_code}_Custom')
                        '''.format(country_code=country_code)
            
            today_df3 = pd.read_sql(sql=sql_str_th, con=hana_connector)

            # if is th, we need to check later that df length is more than 0

            df_length = 0

        if country_code !='TH':
            sql_str_others = f'''
                    Select
                    "PROCESS" AS "SCRIPT","STATUS","START"
                    from "CSM"."PROCESS_EXECUTION_LOGS_SYNAPSE"
                    where
                        "START" >= TO_VARCHAR(NOW(),'YYYY-MM-DD')
                        and "PROCESS" in
                        ('[Daily] Step 2.1 - ZiP Integration Transaction (P).py :: integration_P_perSalesOrgMonthOffset({country_code})',
                        '[Daily] Step 2.2 - ZiP Integration Transaction (CS).py :: integration_CS_perSalesOrgMonthOffset({country_code})')
                    '''.format(country_code=country_code)
            
            today_df3 = pd.read_sql(sql=sql_str_others, con=hana_connector)

            # if is not th, we need to check later that df length is more than 1 as we have 2 status rows

            df_length = 1

        today_df1 = pd.read_sql(sql=sql_str1, con=hana_connector)

        today_df2 = pd.read_sql(sql=sql_str2, con=hana_connector)
        
        # exit and continue to append yesterday data if csm still havent run at 6am

        if dt.datetime.now().hour == 6:

            return -1, 'csm did not run', 'csm did not run', 'csm did not run','csm did not run', dt.datetime.now().hour

        if today_df1.values[0][0] == None or today_df2.values[0][0] == None or len(today_df3)<=df_length:

            time.sleep(random.randint(60,65))

            if dt.datetime.now().minute == 1:
                log1.info(f'Awaiting CSM None, countries: {args.countries}, date_1: {today_df1.values[0][0]}, date_2: {today_df2.values[0][0]}, len_date_3: {len(today_df3)}, today:{today}, time: {dt.datetime.now().replace(microsecond=0)}')
            
            return hana_to_pq_bq.check_hana_csm(hana_connector)
            # return 0, today_df1.values[0][0], today_df2.values[0][0], today

        date_1 = today_df1['(UploadDate)'].apply(parse, dayfirst = False).sort_values(ascending=False).iloc[0].date()

        date_2 = parse(today_df2.values[0][0], dayfirst = False).date()

        today_df3.columns = [x.upper() for x in today_df3.columns]

        date_3 = today_df3.iloc[0]['START'].date()

        status1 = today_df3.iloc[0]['STATUS']

        if country_code!='TH':
            date_4 = today_df3.iloc[1]['START'].date()
            status2 = today_df3.iloc[1]['STATUS']

            if date_1 == date_2 == date_3 == date_4 == today:

                if status1 == 'Successful' and status2 == 'Successful':

                    return 1, date_1, date_2, date_3, status1, today
                
                else:

                    log1.info(f'Awaiting status, countries: {args.countries}, date_1: {date_1}, date_2: {date_2}, date_3: {date_3}, date_4: {date_4}, status1:{status1}, status2:{status2}, time: {dt.datetime.now().replace(microsecond=0)}')
                    
                    time.sleep(random.randint(20,30))

                    return hana_to_pq_bq.check_hana_csm(hana_connector)
                
            else:

                time.sleep(random.randint(60,65))

                if dt.datetime.now().minute == 1:

                    log1.info(f'Awaiting CSM Match Date, countries: {args.countries}, date_1: {date_1}, date_2: {date_2}, date_3: {date_3}, date_4: {date_4}, today:{today}, time: {dt.datetime.now().replace(microsecond=0)}')

                return hana_to_pq_bq.check_hana_csm(hana_connector)

        #if country_code == TH

        if date_1 == date_2 == date_3 == today:

            if status1 == 'Successful':

                return 1, date_1, date_2, date_3, status1, today

            else:

                log1.info(f'Awaiting status, countries: {args.countries}, date_1: {date_1}, date_2: {date_2}, date_3: {date_3}, status1:{status1}, time: {dt.datetime.now().replace(microsecond=0)}')
    
                time.sleep(random.randint(20,30))

                return hana_to_pq_bq.check_hana_csm(hana_connector)

        else:

            time.sleep(random.randint(60,65))

            if dt.datetime.now().minute == 1:
                log1.info(f'Awaiting CSM Match Date, countries: {args.countries}, date_1: {date_1}, date_2: {date_2}, date_3: {date_3}, today:{today}, time: {dt.datetime.now().replace(microsecond=0)}')

            return hana_to_pq_bq.check_hana_csm(hana_connector)

if __name__ == '__main__':

    # load config in .env

    load_dotenv()

    email_receivers1 = os.getenv('email_receivers_list1').split(',')

    email_receivers2 = os.getenv('email_receivers_list2').split(',')

    # create a parser object
    
    parser = argparse.ArgumentParser()

    # add country list arguement e.g. PH,BN

    parser.add_argument("--countries", help="countries list", type=str)

    parser.add_argument("--sales_org_filter", help="sales_org filter", type=str)

    parser.add_argument("--view_name", help="view_name", type=str)

    parser.add_argument("--time_func_name", help="time_func_name", type=str)

    parser.add_argument("--delta_unit", help="delta_unit", type=str)

    parser.add_argument("--halves", help="delta_unit", type=int)

    # create args object to process the args inputs
    
    args = parser.parse_args()

    sales_org_filter = []

    if args.sales_org_filter:
        sales_org_filter = [int(x) for x in args.sales_org_filter.split(',')]
    
    # detailed log for sub-processes throughout the script

    log1 = log_func.log_it("ctry_logs")

    # overall log for completion of entire process

    log2 =  log_func.log_it("process_log")

    log1.info(f'Checking CSM, countries: {args.countries}')

    log2.info(f'Checking CSM, countries: {args.countries}')

    for i1 in range(3): 

        try:

            hana_connector = hana_connector_utils.instantiate_hana_conn_bdp(os.getenv('hana_details_path'))

            #for i in range(300):

            #sleep_duration = random.randint(55,60)

            csm_ok, date_1, date_2, date_3, status1, today = hana_to_pq_bq.check_hana_csm(hana_connector)

            #csm_ok = 1

            if csm_ok == 1:

                log1.info(f'CSM Check Done, countries:{args.countries}, date_1: {date_1}, date_2: {date_2}, date_3: {date_3}, status1: {status1}, today:{today}, i1:{i1}, time: {dt.datetime.now().replace(microsecond=0)}')

                break

            if csm_ok == -1:

                log1.info(f'Exit CSM proceeding to yesterday, country:{args.countries},  date_1: {date_1}, date_2: {date_2}, date_3: {date_3}, status1: {status1}, today:{today}, i1:{i1}, csm_ok:{csm_ok}, time: {dt.datetime.now().replace(microsecond=0)}')

                break

        except Exception as exp:

            log1.exception(f'Checking CSM error, countries: {args.countries}, fail on  try {i1} \n> {exp}')

            log2.info(f'Checking CSM error, countries: {args.countries}, fail on  try {i1}')

            time.sleep(random.randint(55,60))

    try:

        start_time = time.time()

        run_started = dt.datetime.now().replace( microsecond=0)

        # log1.info(f'RS: {run_started} Yesterday hana query started, countries: {args.countries}')

        # first, we query for 1 day's data and append to bq table

        bq_migration_obj = hana_to_pq_bq(args.countries)

        # get config variables from excel

        bq_migration_obj.get_sql_config(view_name = args.view_name)

        yesterday1 = (dt.datetime.now().date()-dt.timedelta(1)).strftime('%Y%m%d')

        bq_migration_obj.period_start = [yesterday1]
        bq_migration_obj.period_end = [yesterday1]

        bq_migration_obj.query_hana_to_pq(view_name = args.view_name, sales_org_filter = sales_org_filter, suffix='yesterday')

        # get current python filename so we can reference the upload_to_bq filename and import it

        current_filename = os.path.basename(__file__).split('.')[0]

        upload_filename = current_filename + '_upload_to_bq'

        bq_connector_module = importlib.import_module(upload_filename)

        bq_connector1 = bq_connector_module.bq_connector(args.countries)

        # as the bq_upload file might be write_truncate, we force it to append here

        write_options = ['write_append','write_append']

        # get config from excel. Here, if args.halves is defined, then we only append yesterday's data when is the first half and not second half
        # if we upload second half too which is ran on another vm there will be dupicated uploads
        # if args.halves is not defined, then this will be ignored

        if args.halves !=2:

            bq_connector1.get_bq_config(args.view_name, write_options)

            bq_connector1.upload_folder_to_bq(delta_unit = args.delta_unit, time_func_name = args.time_func_name, adhoc = 'yes', country_list = bq_migration_obj.country_list,)

        log1.info(f'RS: {run_started} 3 Yesterday files bq upload, countries: {bq_migration_obj.country_list}, sales_org_filter: {sales_org_filter}, succeeded')

        log2.info(f'RS: {run_started} Yesterday files bq upload, countries: {bq_migration_obj.country_list}, sales_org_filter: {sales_org_filter}, succeeded')

    except Exception as exp:

        log1.exception(f'RS: {run_started} 3 Yesterday hana query to pq, countries: {bq_migration_obj.country_list}, sales_org_filter: {sales_org_filter}, fail \n> {exp}')
    
        log2.info(f'RS: {run_started} Yesterday hana query to pq, countries: {bq_migration_obj.country_list}, sales_org_filter: {sales_org_filter}, fail')

    # exiting here after we appended yesterday files as we do not want to run full refresh if csm did not run
    
    if i1==2 or csm_ok==-1:

        random_utils.Emailer().send_it(receivers = email_receivers2, subject = f'Checking CSM Exit after yesterday upload - {args.view_name} - {args.countries}' , content = f'hostname: {socket.gethostname()} <br /> fail on csm_ok: {csm_ok}, try {i1} <br />')

        log1.info(f'CSM exit after yesterday upload, countries: {args.countries}, csm_ok: {csm_ok}, try: {i1}')

        log2.info(f'CSM exit after yesterday upload, countries: {args.countries}, csm_ok: {csm_ok}, try: {i1}')

        sys.exit()

    try:

        ''' Here is the logic for the full refresh'''
    
        # print(f'\nNOTE: Hana query to pq started run {run_started} {dt.datetime.now().replace( microsecond=0)}\n')

        # activate env variables and path
                
        bq_migration_obj = hana_to_pq_bq(args.countries)

        # get config variables from excel

        bq_migration_obj.get_sql_config(view_name = args.view_name)

        # determine if we want to backtrack by months or years

        yesterday = (dt.datetime.now().date()-dt.timedelta(days=1))

        # get list of time periods

        bq_migration_obj.period_start, bq_migration_obj.period_end = time_func.get_time_periods(delta = bq_migration_obj.delta, delta_unit= args.delta_unit, time_func_name= args.time_func_name, end_date = yesterday,)

        # below lines is to force a period start and period end

        # query hana and save to pq

        #bq_migration_obj.period_start = ['20221105']
        #bq_migration_obj.period_end = ['20221110']

        bq_migration_obj.query_hana_to_pq(view_name = args.view_name, sales_org_filter = sales_org_filter)

        # print(f'\nNOTE: function ended {dt.datetime.now().replace(microsecond=0)}\n')

        end_time = time.time()

        run_time = end_time - start_time

        log1.info(f'RS: {run_started} Hana query to pq {bq_migration_obj.country_list}, sales_org_filter: {sales_org_filter}  {__name__}, run_time: {time_func.get_time_hh_mm_ss(run_time)}, completed successfully')

        log2.info(f'RS: {run_started} Hana query to pq {bq_migration_obj.country_list}, sales_org_filter: {sales_org_filter}  {__name__}, run_time: {time_func.get_time_hh_mm_ss(run_time)}, completed successfully')

    except Exception as exp:

        log1.exception(f'RS: {run_started} Hana query to pq {bq_migration_obj.country_list}, sales_org_filter: {sales_org_filter}  {__name__}, fail \n> {exp}')
    
        log2.info(f'RS: {run_started} Hana query to pq {bq_migration_obj.country_list}, sales_org_filter: {sales_org_filter}  {__name__}, fail')