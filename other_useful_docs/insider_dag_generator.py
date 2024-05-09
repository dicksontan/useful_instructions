from airflow import DAG
import datetime as dt
import pandas as pd
import time
import os
import random
import requests
import urllib
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable,DagRun
from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator
from airflow.providers.microsoft.azure.sensors.data_factory import AzureDataFactoryPipelineRunStatusSensor
from airflow.providers.microsoft.azure.hooks.azure_data_lake import AzureDataLakeHook
from azure.storage.filedatalake import DataLakeServiceClient
from io import BytesIO
from jinja2 import Template
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.exceptions import AirflowFailException
from zdt import time_func, connector_utils

def generate_dag(excel_config_file_path="config_files/bq_sql_config_databricks_test.xlsx", **context):

    # filter the rows we want to create dag

    #config input: {"run_list": ["y","z"]}

    run_list = context['dag_run'].conf['run_list'] 

    save_list = ['__pycache__']

    excel_config_df = pd.read_excel(connector_utils.get_azure_file_bytes(account_key = 'default_airflow',variable_name = 'decaairflowdev_var1', account_name = 'dataengnrcaairflowdev',file_path = excel_config_file_path)) 

    dags_folder_path="/opt/airflow/dags/generated_dags_folder/"

    if not os.path.isdir(dags_folder_path):
        os.makedirs(dags_folder_path)

    target_runs = excel_config_df[excel_config_df['to_generate'].isin(run_list)]

    if 'y' in run_list:
        # delete and regenerate dags
        for f in os.listdir(dags_folder_path):
            if f not in save_list:
                fpath = dags_folder_path + f
                os.remove(fpath)
    
    print(10, target_runs)

    # for test runs
    #target_runs = excel_config_df[(excel_config_df['to_generate']=='y') & (excel_config_df['unique_id']=='sales_flash')]

    for index, row in target_runs.iterrows():

        unique_id = row['unique_id']

        # time_start = dt.datetime.now().replace(microsecond=0)+dt.timedelta(minutes=2)

        template = Template("""
from airflow import DAG
import datetime as dt
import pandas as pd
import time
import os
import random
import requests
import urllib
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable, DagRun
from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator
from airflow.providers.microsoft.azure.sensors.data_factory import AzureDataFactoryPipelineRunStatusSensor
from airflow.providers.microsoft.azure.hooks.azure_data_lake import AzureDataLakeHook
from azure.storage.filedatalake import DataLakeServiceClient
from io import BytesIO
from jinja2 import Template
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.exceptions import AirflowFailException
from zdt import time_func, connector_utils
from ast import literal_eval
from datetime import timedelta
from typing import List, Tuple
from airflow.models.taskinstance import TaskInstance
import pytz


#---------------------- Defining Variables-------------------------------
unique_id = "{{ unique_id }}"
#time_start = "{{ time_start }}"
run_start = (dt.datetime.now()+dt.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")

excel_config_file_path="config_files/bq_sql_config_databricks.xlsx"
excel_config_df = pd.read_excel(connector_utils.get_azure_file_bytes(account_key = 'default_airflow',variable_name = 'decaairflowdev_var1', account_name = 'dataengnrcaairflowdev',file_path = excel_config_file_path))
                            
target_row_df = excel_config_df[excel_config_df.unique_id== unique_id]
lookup_sql= target_row_df.lookup_sql.values[0]
table_name=target_row_df['unique_id'].values[0]
generated_dag_schedule_interval = target_row_df.cron_expression.values[0]
airflow_tag = target_row_df.airflow_tag.values[0]
add_on = target_row_df.add_on.values[0]
odbc_partition = target_row_df.odbc_partition.values[0]
partition_by_field = target_row_df.bq_partition_field.values[0]
cluster_by_field = target_row_df.bq_cluster_field.values[0].lower()
bq_looker_view = target_row_df.bq_looker_view.values[0]
bq_final_table1 = target_row_df.bq_final_table.values[0]
bq_duplicate_sql_statement = target_row_df.bq_duplicate_sql_statement.values[0]
special_db = target_row_df.special_db.values[0]
schema_name = airflow_tag

azure_data_factory_conn_id = "azure_data_factory"
azure_factory_name = "CSM-DEV"
azure_resource_group_name = "CSM"
airflow_email = 'dtands@zuelligpharma.com'
adf_import_data_pipeline_name = 'import_data_new'
adf_ship_data_pipeline_name = 'insider_ship_main_new'

if special_db != 'nil':
    special_db = literal_eval(special_db)

    if 'other_db' in special_db:
        adf_import_data_pipeline_name=adf_import_data_pipeline_name+ f"_{special_db['other_db']}"

    if schema_name=='eztracker':
        adf_ship_data_pipeline_name=adf_ship_data_pipeline_name+ f"_{special_db['other_db']}"

BQ_CONN_ID = 'my_gcp_conn'
BQ_PROJECT = 'zp-dev-looker-analytics'
BQ_DATASET = 'insider_dev'

csm_required_suc = 21

#------------------------------------------------------------------------

def csm_poke():

    '''creating path to imported csm status parquet files'''

    container_name = 'raw'
    day = str(dt.datetime.now().day).zfill(2)
    directory_name = f"BDP/check_csm/day={day}/"

    path_list = connector_utils.get_azure_file_paths(container_name=container_name,directory_name=directory_name)

    output_list = []

    print(10,path_list)

    '''creating parquet files from path and create df'''

    for file_path in path_list:

        print(11, file_path)

        file_path1 = f"check_csm/day={day}/{file_path}"

        df = pd.read_parquet(connector_utils.get_azure_file_bytes(account_key = 'placeholder',variable_name = 'decaairflowdev_var1', account_name = 'dataengnrcaairflowdev',file_path = file_path1))

        output_list.append(df)

    final_df = pd.concat(output_list)

    return final_df

def call_check_csm_adf(required_suc,min_suc_to_run,**context):

    azure_pipeline_args = {
    'task_id': 'run_check_csm_import_data_task_id',
    'pipeline_name': 'check_csm_import_data',
    "azure_data_factory_conn_id": azure_data_factory_conn_id,
    "factory_name": azure_factory_name,
    "resource_group_name": azure_resource_group_name,
    "retries": 1, 
    'retry_delay': dt.timedelta(minutes=1)}

    '''run pipeline to import csm status data as parquet files'''

    import_data_stage = AzureDataFactoryRunPipelineOperator(**azure_pipeline_args)

    '''checking status'''
    suc_count = 0

    # hour 21 is 5am sgt. force run alternate branch if it doesnt work at 5am

    while suc_count < required_suc and time_func.astimezone_sg(dt.datetime.now()).hour!= 5:

        import_data_stage.execute(context)

        final_df1 = csm_poke()

        print(12, final_df1, f'suc_count: {suc_count}')

        if final_df1.empty==False:

            suc_count = final_df1.STATUS.value_counts()['Successful']

        time.sleep(10)

    print(13, f'suc_count: {suc_count}, datetime: {dt.datetime.now().replace(microsecond=0)}')

    # branch calling task_ids of next steps
    if suc_count == required_suc:
        return 'print_date1'
    
    elif suc_count >= min_suc_to_run and time_func.astimezone_sg(dt.datetime.now()).hour== 5:
    
        file_name = os.path.basename(__file__)
    
        message = {'exit_message_value':f'min_suc_ran {min_suc_to_run}','source': file_name}
    
        context['ti'].xcom_push(key='exit_message_key',value = message)

        return 'print_date1'

    else:
    
        file_name = os.path.basename(__file__)
    
        message = {'exit_message_value':'csm full exit','source': file_name}
    
        context['ti'].xcom_push(key='exit_message_key',value = message)
        
        return 'throw_exception_task_id'

def change_cron_hour(cron_str, hours = 8):

    cron_str = cron_str.lower()

    dates_dict = {'sun': '0', 'mon': '1', 'tue': '2', 'wed': '3', 'thu': '4', 'fri': '5', 'sat': '6'}

    for day,value in dates_dict.items():
        cron_str=cron_str.replace(day,value)

    cron_list = cron_str.split()
    if cron_str == 'nil':
        return None
    
    try:

        num = int(cron_list[1])

        num -= hours

        if num<0:
            num+=24

            cron_list_day_of_month = cron_list[2]

            cron_list_day_of_week = cron_list[-1]

            if '-' in cron_list_day_of_month:
                cron_list_day_of_month = re.split(r'(-)', cron_list[2])
            
            if ',' in cron_list_day_of_month:
                cron_list_day_of_month = re.split(r'(,)', cron_list[2])

            #cron_list_day_of_week = cron_list[-1].split('-')

            if '-' in cron_list_day_of_week:
                cron_list_day_of_week = re.split(r'(-)', cron_list[-1])
            
            if ',' in cron_list_day_of_week:
                cron_list_day_of_week = re.split(r'(,)', cron_list[-1])
                    
            cron_list_day_of_week_list = []
            
            for i in range(len(cron_list_day_of_week)):
                        
                try:

                    day = str(int(cron_list_day_of_week[i])-1)

                    if day=='-1':
                        day='6'

                    cron_list_day_of_week_list.append(day)

                except:
                    cron_list_day_of_week_list.append(cron_list_day_of_week[i])
                    continue

            cron_list_day_of_month_list = []
            
            for i in range(len(cron_list_day_of_month)):
                        
                try:

                    day = str(int(cron_list_day_of_month[i])-1)

                    if day=='0':
                        day='1'

                    if day in cron_list_day_of_month_list:

                        cron_list_day_of_month_list.append(str(int(day)+1))

                    else:
                        cron_list_day_of_month_list.append(day)

                except:

                    cron_list_day_of_month_list.append(cron_list_day_of_month[i])

                    continue

            cron_list_day_of_week1 = ''.join(cron_list_day_of_week_list)

            if '-' in cron_list_day_of_week1 and cron_list_day_of_week1[0]=='6':
                
                cron_list_day_of_week1 = ','.join([str(x) for x in range(int(cron_list_day_of_week1[2])+1)]+['6'])

            cron_list_day_of_month1 = ''.join(cron_list_day_of_month_list)

            cron_list[-1] = cron_list_day_of_week1

            cron_list[2] = cron_list_day_of_month1

        cron_list[1] = str(num)

        cron_str = ' '.join(cron_list)

        return cron_str
    
    except:

        return cron_str

def throw_exception_task_func(**context):

    message1 = context['ti'].xcom_pull(key='exit_message_key')

    if message1 is not None:
    
        exit_message = message1['exit_message_value']
        source = message1['source']

        raise AirflowFailException(f"Message: Exit at {exit_message}, Source: {source}")
    
    else:
        return 'no errors'

def push_notif_teams(dag_id, task_id, tree_url, params, run_id,dag_start_time,webhook_url,status):

    '''
    - Structure of notification to be sent
    - Returns the status code of the HTTP request
      - dag_id : ID of the dag that this function is being applied to
      - task_id : ID of the task in the dag that this function is being applied to
      - tree_url : url to the airflow tree page
      - params : shows if there are any parameters being used in the dag run
      - run_id : ID of the run that this function is being applied to
      - color (optional) : hexadecimal code of the notification's top line color, default corresponds to black
    '''

    title = f"DAG {dag_id} has {status}"
    content = f'''
    DAG: {dag_id}
    Run ID: {run_id}
    Dag_start_time: {dag_start_time}
    Paramas Used: `{params}`
    To view the tree for this task, click on:
    '''
    color = "FF0000"

    response = requests.post(url = webhook_url, headers = {"Content-Type": "application/json"},
                             json = {"themeColor": color,
                                 "summary": title,
                                 "sections": [{
                                     "activityTitle": title,
                                     "activitySubtitle": content,
                                     "potentialAction": [{"@type": "OpenUri",
                                          "name": "View Tree",
                                          "targets": [{"os":"default", "uri": tree_url}]
                                         }]
                                 }]
                             }
                             )

    if response.status_code !=200:
        print(f'Notification sending failed with code: {response.status_code}')
        print(f'This is the content: {response.content}')
    
    else:
        print('Notification sent successfully')

    return response.status_code 

def send_teams_status(context,status='failed'):
    '''Teams alerting. It will go on_failure => push_notif'''

    dag_id = context['dag_run'].dag_id
    task_id = context['task_instance'].task_id
    url_date = urllib.parse.quote(context['ts'])
    #context['ts'] looks like 2023-05-04T01:49:56.150770+00:00 
    #url_date looks like 2023-05-04T01%3A49%3A56.150770%2B00%3A00
    params = context['params']
    run_id = context['run_id']
    reason = context.get('reason')
    params['reason'] = reason
    dag_start_time = time_func.astimezone_sg(dt.datetime.fromisoformat(run_id.split('__')[1])).strftime("%Y%m%d-%H:%M:%S")
    tree_url = f"https://e5cf5975741683.southeastasia.airflow.svc.datafactory.azure.com/dags/{dag_id}/grid"
    webhook_url_prod = "https://zpssgpatientsolutions.webhook.office.com/webhookb2/cf91203b-52a3-420e-8e3c-192e870c1927@19cff0af-7bfb-4dfc-8fdc-ecd1a242439b/IncomingWebhook/7ce7101e5982491da7414c178ecd209e/d8c04f7b-19c8-415f-97a7-b17e4072c703"
    webhook_url_dev = "https://zpssgpatientsolutions.webhook.office.com/webhookb2/cf91203b-52a3-420e-8e3c-192e870c1927@19cff0af-7bfb-4dfc-8fdc-ecd1a242439b/IncomingWebhook/31b2133db0d34598acb5e781168d96b7/c77ad693-3bbd-4968-9016-49d28da85d71"
    webhook_url_ca =  "https://zpssgpatientsolutions.webhook.office.com/webhookb2/378fec86-6610-4937-b0dd-6fd7d96e8f98@19cff0af-7bfb-4dfc-8fdc-ecd1a242439b/IncomingWebhook/28bc033aa5f543cdaeb5e81a416f91ee/c77ad693-3bbd-4968-9016-49d28da85d71"
    
    if status=='failed' and not run_id.startswith('manual__'):
        result_code = push_notif_teams(dag_id, task_id, tree_url, params, run_id,dag_start_time,webhook_url_dev,status) 
    
    excel_config_df = pd.read_excel(connector_utils.get_azure_file_bytes(account_key = 'default_airflow',variable_name = 'decaairflowdev_var1', account_name = 'dataengnrcaairflowdev',file_path = "config_files/bq_sql_config_databricks.xlsx"))

    target_row = excel_config_df[excel_config_df['unique_id']==dag_id.split('_generated_dag')[0]]

    if target_row.prod_status.values[0]=='prod' and status == 'failed':

        result_code = push_notif_teams(dag_id, task_id, tree_url, params, run_id,dag_start_time,webhook_url_prod,status)


    if run_id.startswith('manual__'):
                            
        #if it failed, it will still go here and push to de_ca_run url as default value is failed
                            
        dag_runs = DagRun.find(dag_id=dag_id)
    
        # Filter runs that are in a 'success' state
        successful_runs = [run for run in dag_runs if run.state == 'success']

        total_duration = sum([(run.end_date - run.start_date).total_seconds() for run in successful_runs if run.start_date and run.end_date])
        mean_duration = total_duration / len(successful_runs) if successful_runs else 0

        def convert_seconds_to_hms(seconds):
            hours, remainder = divmod(seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            return int(hours), int(minutes)

        # Assuming mean_duration is in seconds
        mean_hours, mean_minutes = convert_seconds_to_hms(mean_duration)

        params['avg_runtime']= f'{mean_hours} hours, {mean_minutes} minutes'

        result_code = push_notif_teams(dag_id, task_id, None, params, run_id,dag_start_time,webhook_url_ca,status)

    print(f"result code of MS teams notification webhook: {result_code}")            

jinja_dag_args = {
'depends_on_past': False,
'email_on_failure': True,
'email_on_retry': True,
'retries': 1,
'retry_delay': dt.timedelta(minutes=1),
'email': ['dtands@zuelligpharma.com'],
"azure_data_factory_conn_id": azure_data_factory_conn_id,
"factory_name": azure_factory_name, 
"resource_group_name": azure_resource_group_name,
"sla": dt.timedelta(minutes=180),
}

generated_dag_name = unique_id.replace("/","-").replace(".","-")

def send_teams_request(dag: DAG,
    task_list: str,
    blocking_task_list: str,
    slas: List[Tuple],
    blocking_tis: List[TaskInstance]):
    print("on sla miss started")
    env = airflow_tag
    dag_id = dag.dag_id
    execution_date = dag.get_latest_execution_date().isoformat().replace(" ", "+")
    task_id = slas[0].task_id
    sla_miss_items = '+'.join([repr(x) for x in slas])

    tags = [
        "app:airflow",
        f"env:{env}",
        f"airflow_dag_id:{dag_id}",
        f"airflow_run_id:{execution_date}",
    ]

    execution_date = urllib.parse.quote(execution_date)
    logs_url = f"https://e5cf5975741683.southeastasia.airflow.svc.datafactory.azure.com/log?dag_id={dag_id}&task_id={task_id}&execution_date={execution_date}"
    webhook_url_dev = "https://zpssgpatientsolutions.webhook.office.com/webhookb2/cf91203b-52a3-420e-8e3c-192e870c1927@19cff0af-7bfb-4dfc-8fdc-ecd1a242439b/IncomingWebhook/31b2133db0d34598acb5e781168d96b7/c77ad693-3bbd-4968-9016-49d28da85d71"

    title = f"DAG SLA missed: {dag_id}"
    content = f"Tag: {tags} The following tasks have missed their SLA {blocking_task_list} {sla_miss_items} {logs_url}"
    
    requests.post(
        url=webhook_url_dev,
        headers={"Content-Type": "application/json"},
        json={
            "themeColor":  "FF0000",
            "summary": title,
            "sections": [
                {
                    "activityTitle": title,
                    "activitySubtitle": content,
                    "potentialAction": [
                        {
                            "@type": "OpenUri",
                            "name": "View logs",
                            "targets": [{"os": "default"}],
                        }
                    ],
                }
            ],
        },
    )

generated_jinja_dag = DAG(f'{generated_dag_name}_generated_dag',
    #start_date= dt.datetime.strptime(time_start, '%Y-%m-%d %H:%M:%S'),
    start_date= dt.datetime(2018,1,1, tzinfo=pytz.timezone('Asia/Singapore')),
    max_active_runs=1,
    default_args= jinja_dag_args,
    #schedule_interval= change_cron_hour(generated_dag_schedule_interval),
    schedule_interval=None if generated_dag_schedule_interval=='nil' else generated_dag_schedule_interval,
    catchup=False,
    is_paused_upon_creation=True,
    on_failure_callback= send_teams_status,
    tags=[airflow_tag],
    sla_miss_callback=send_teams_request,
    dagrun_timeout=dt.timedelta(minutes=360)
    )
                            
# https://airflow.apache.org/docs/apache-airflow/2.6.3/core-concepts/tasks.html#sla-miss-callback

# def send_suc_status(**context):

#     send_teams_status(context,'succeded')
    
def send_suc_status(**context):

    run_id = context['run_id']
    
    if run_id.startswith('manual__'):

        send_teams_status(context,'succeded')

send_suc_status_task = PythonOperator(task_id='send_suc_status_task_id',
                                      python_callable=send_suc_status,
                                    dag = generated_jinja_dag,
                                      #trigger_rule='all_success'
                                       )
                                                     
def send_start_status(**context):

    run_id = context['run_id']
    
    if run_id.startswith('manual__'):

        send_teams_status(context,'started')
                            
send_start_status_task = PythonOperator(task_id='send_start_status_task_id',
                                      python_callable=send_start_status,
                                    dag = generated_jinja_dag,
                                      #trigger_rule='all_success'
                                      )

date_0 = BashOperator(
    task_id="start_print_date",
    bash_command="date", dag = generated_jinja_dag)           

import_parameters=dict(lookup_sql= lookup_sql,
     table_name=table_name,
     odbc_partition=odbc_partition,
     run_start = run_start,
     schema_name=schema_name)
    
insider_import_data_run = AzureDataFactoryRunPipelineOperator(
    task_id= 'import_data_' + unique_id,
    pipeline_name=adf_import_data_pipeline_name, 
    dag=generated_jinja_dag,
    trigger_rule='one_success',
    parameters=import_parameters
    )

total_imports_list = []
total_imports_list.append(insider_import_data_run)            

if 'add_import' in add_on:

    add_imports_list = add_on.split(':')[1].split(',')

    for add_import_unique_id in add_imports_list:
    
        target_row_df_ai = excel_config_df[excel_config_df.unique_id== add_import_unique_id]
        lookup_sql_ai= target_row_df_ai.lookup_sql.values[0]
        #table_name_ai=target_row_df_ai['unique_id'].values[0]
        odbc_partition_ai= target_row_df_ai['odbc_partition'].values[0]
        
        parameters_ai=dict(lookup_sql= lookup_sql_ai,
        table_name=table_name,
        odbc_partition=odbc_partition_ai,
        schema_name=schema_name)

        insider_import_data_run_ai = AzureDataFactoryRunPipelineOperator(
            task_id= 'import_data_' + add_import_unique_id,
            pipeline_name= adf_import_data_pipeline_name, 
            dag=generated_jinja_dag,
            trigger_rule='one_success',
            #on_failure_callback= send_teams_status,
            parameters=parameters_ai)

        total_imports_list.append(insider_import_data_run_ai)

ship_parameters=dict(table_name=table_name, run_start=run_start)    

insider_ship_pipe_run = AzureDataFactoryRunPipelineOperator(
    task_id= 'run_ship_main_task_id',
    pipeline_name= adf_ship_data_pipeline_name, 
    dag=generated_jinja_dag,
    trigger_rule='all_success',
    #on_failure_callback= send_teams_status,
    parameters=ship_parameters)
        
# Run BQ

bq_duplicate_job = BigQueryOperator(
        task_id='bq_duplicate_task_id',
        sql= bq_duplicate_sql_statement.format(bq_final_table=bq_final_table1,bq_looker_view=bq_looker_view,partition_by_field= partition_by_field,cluster_by_field=cluster_by_field ),
        use_legacy_sql=False,
        gcp_conn_id=BQ_CONN_ID,
        #trigger_rule = 'all_success',
        #on_failure_callback= send_teams_status,
        dag=generated_jinja_dag)

if add_on == 'check_csm':

    csm_required_suc = csm_required_suc
    min_suc_to_run = 10

    call_csm_branch_op = BranchPythonOperator(task_id='check_csm_task_id',
                            python_callable=call_check_csm_adf, 
                            op_kwargs={'required_suc': csm_required_suc,
                            'min_suc_to_run': min_suc_to_run},
                            dag = generated_jinja_dag)

    throw_exception_task = PythonOperator(dag=generated_jinja_dag,
        #trigger_rule='all_done',
        trigger_rule = 'all_success',
        task_id='throw_exception_task_id',
        python_callable=throw_exception_task_func)

    date_1 = BashOperator(
        task_id="print_date1",
        bash_command="date", dag = generated_jinja_dag) 

                            
    date_0 >> send_start_status_task >> call_csm_branch_op >> date_1 >> total_imports_list >> insider_ship_pipe_run >> throw_exception_task >> bq_duplicate_job >> send_suc_status_task

    date_0 >> call_csm_branch_op >> throw_exception_task

else:

    date_0 >> send_start_status_task >> total_imports_list >> insider_ship_pipe_run >> bq_duplicate_job >> send_suc_status_task

    """)
        
        #rendered_template = template.render(unique_id= unique_id,time_start=time_start)
        rendered_template = template.render(unique_id= unique_id)

        path = dags_folder_path + unique_id + '_dag.py'

        # path = "/opt/airflow/dags/"+ unique_id + '_dag.py'

        print(15,path)

        with open(path,'w') as f:
            f.write(rendered_template)
#--------------------------dag generator variables-----------------------------------

excel_config_file_path = "config_files/bq_sql_config_databricks.xlsx"

#------------------------------------------------------------------------------------

generator_dag_params = {
    'dag_id': 'dynamic_dag_generator_1',
    'start_date': dt.datetime(2023, 1, 1),
    'schedule_interval': None,
    'catchup': False
}  

# Define the DAG and its tasks
with DAG(**generator_dag_params) as dag:
    generate_dag = PythonOperator(
        task_id='generate_dag',
        python_callable=generate_dag,
        op_kwargs={'excel_config_file_path': excel_config_file_path},
        #provide_context=True
    )

generate_dag