from sqlalchemy import create_engine
from typing import Any
from hdbcli import dbapi
import time
import random

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