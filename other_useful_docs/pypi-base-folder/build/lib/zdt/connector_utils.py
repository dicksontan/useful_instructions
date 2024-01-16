from sqlalchemy import create_engine
from typing import Any
from hdbcli import dbapi
import time
import random
from airflow.models import Variable
from azure.storage.filedatalake import DataLakeServiceClient
from io import BytesIO

def instantiate_hana_conn_bdp(password_file) -> Any:

    for i in range(3):

        sleep_duration = random.randint(1,10)

        try:

            with open(password_file) as f:
                login = [line.rstrip() for line in f]
                
                engine_sap = create_engine('hana://'+login[0]+':'+login[1]+ '@zpsgbdphanadb:32415')

                conn_sap = engine_sap.connect()

                print(f'{engine_sap} connection done')

            return conn_sap

        except:

            print(f'connection failed {i}')

            time.sleep(sleep_duration)

def instantiate_hana_conn_hip(password_file) -> Any:

    for i in range(3):

        sleep_duration = random.randint(1,10)

        try:

            with open(password_file) as f:
                login = [line.rstrip() for line in f]
                
                engine_sap = create_engine('hana://'+login[2]+':'+login[3]+ '@10.10.1.65:38841')

                conn_sap = engine_sap.connect()

                print(f'{engine_sap} connection done')

            return conn_sap

        except:

            print(f'connection failed {i}')

            time.sleep(sleep_duration)

def instantiate_hana_conn_bdp2(password_file) -> Any:

    for i in range(3):

        sleep_duration = random.randint(1,10)

        try:

            with open(password_file) as f:
                login = [line.rstrip() for line in f]
                
                conn_sap = dbapi.connect(
                address="zpsgbdphanadb",
                port=32415,
                user=login[0],
                password=login[1])

                conn_hdbcli = conn_sap.cursor()

                print(f'{conn_sap} connection done')

            return conn_hdbcli

        except:

            print(f'connection failed {i}')

            time.sleep(sleep_duration)

def get_azure_file_bytes(account_key = 'default_airflow', variable_name = 'dag_var1', account_name = 'zpcsmdevdatalake',container_name = 'raw', directory_name = "BDP",file_path = "schema_name=zip_insider/bq_sql_config_databricks.csv"):

    '''If account_key = default_airflow, then we will get from airflow variables'''

    if account_key == 'default_airflow':

        variable_list = Variable.get(variable_name, deserialize_json=True)

        account_key = variable_list['acc_key']

    service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", account_name), credential=account_key)
    
    file_system_client = service_client.get_file_system_client(file_system=container_name)

    directory_client = file_system_client.get_directory_client(directory_name)

    file_client = directory_client.get_file_client(file_path)

    download = file_client.download_file()

    downloaded_bytes = download.readall()

    return BytesIO(downloaded_bytes)

def get_azure_file_paths(account_key = 'default_airflow', variable_name = 'dag_var1', account_name = 'zpcsmdevdatalake',container_name = 'raw', directory_name = "BDP/check_csm/",):
 
    '''If account_key = default_airflow, then we will get from airflow variables'''

    if account_key == 'default_airflow':

        variable_list = Variable.get(variable_name, deserialize_json=True)

        account_key = variable_list['acc_key']

    service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", account_name), credential=account_key)
    
    file_system_client = service_client.get_file_system_client(file_system=container_name)

    paths = file_system_client.get_paths(path=directory_name)

    path_list = [*paths]

    path_list2=[path.name for path in path_list]

    path_list3 = [x.split(directory_name)[-1] for x in path_list2]

    return path_list3