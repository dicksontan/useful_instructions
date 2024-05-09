import logging
import pathlib
import datetime as dt
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from airflow.operators.email_operator import EmailOperator
from kubernetes.client import models as k8s
from zpcommon.utils import (
    create_on_failure_callback,
    create_on_sla_miss_callback,
    create_on_success_callback,
    get_node_settings,
)
import pytz
from airflow.exceptions import AirflowException
import time
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
import requests

ENV="dev"
airflow_tag =['vm_migration','datafeed']

with open(pathlib.Path(__file__).parent.resolve().absolute().joinpath("etl-image-dev.txt")) as f:
    ETL_DOCKER_IMAGE = f.readline()

def print_input(input_value):
    task_logger.info("print_input started")
    print(f"Image used: {input_value}")

task_logger = logging.getLogger("airflow.task")

def push_success_message(context):
    ti = context['ti']
    task_id = ti.task_id
    ti.xcom_push(key=f"{task_id}_success", value="success!!")

# configmaps = [
#     k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name=f"dataeng-looker-config-etls-{ENV}")),
# ]


def push_failure_message(context):
    ti = context['ti']
    task_id = ti.task_id
    ti.xcom_push(key=f"{task_id}_success", value="failed!")

default_args = {
    'retries': 0,
    'retry_delay': dt.timedelta(minutes=1),
    #"sla_miss_callback": create_on_sla_miss_callback1(),
}

dag1 = DAG(
    'datafeed_abbott_insider_dag2', 
    default_args=default_args,
    start_date=dt.datetime(2018, 1, 1, tzinfo=pytz.timezone('Asia/Singapore')),
    max_active_runs=1,
    schedule_interval=None,
    catchup=False,
    is_paused_upon_creation=True,
    tags=airflow_tag,
    #dagrun_timeout=dt.timedelta(seconds=10),
)

print_input_task = PythonOperator(
    task_id='print_input_task',
    python_callable=print_input,
    op_kwargs={'input_value': ETL_DOCKER_IMAGE},
    #sla= dt.timedelta(seconds=2),
    on_success_callback=push_success_message,
    on_failure_callback=push_failure_message,
    dag=dag1
)

datafeed_insider_task = KubernetesPodOperator(
    namespace="airflow",
    image=ETL_DOCKER_IMAGE,
    image_pull_secrets=[k8s.V1LocalObjectReference("airflow-docker-secret")],
    cmds=[
        "poetry",
        "run",
        "python",
        "src/app/abbott/abbott_insider/datafeed_insider_main.py",
        "-c",
        "MY",
        "-datafeed",
        "TRANSACTIONAL",
        "-datafeed_type",
        "DAILY",
        "-env",
        f"{ENV}"
    ],
    is_delete_operator_pod=True,
    in_cluster=True,
    task_id="datafeed_insider_task",
    get_logs=True,
    image_pull_policy="Always",
    log_events_on_failure=True,
    **get_node_settings("airflow16"),
    container_resources=k8s.V1ResourceRequirements(
        limits={"memory": "2000M", "cpu": "400m"},
        requests={"memory": "600M", "cpu": "200m"},
    ),
    #sla= dt.timedelta(seconds=2),
    on_success_callback=push_success_message,
    on_failure_callback=push_failure_message,
    dag=dag1
)

# def pulling_xcom(context):
#     message = context['ti'].xcom_pull(key='exit_message_key')
#     return message

raise_error_task= KubernetesPodOperator(
        namespace="airflow",
        image=ETL_DOCKER_IMAGE,
        image_pull_secrets=[k8s.V1LocalObjectReference("airflow-docker-secret")],
        cmds=[
            "poetry", 
            "run",
            "python",
            "pipelines/callbacks/on_failure.py", 
            "--env",
            f"{ENV}",
            "--email_receivers",
            "dtands@zuelligpharma.com",
            "--cc_receivers",
            "dtands@zuelligpharma.com,dtands@zuelligpharma.com",
            "--dag_id",
            "{{ dag.dag_id }}",
        ],
        is_delete_operator_pod=True,
        in_cluster=True,
        task_id="raise_error_task",
        get_logs=True,
        image_pull_policy="Always",
        log_events_on_failure=True,
        **get_node_settings("airflow16"),
        container_resources=k8s.V1ResourceRequirements(
            limits={"memory": "2000M", "cpu": "400m"},
            requests={"memory": "600M", "cpu": "200m"},
        ),
        dag=dag1,
        trigger_rule='one_failed',
    )

class AllTasksSuccessSensor(BaseSensorOperator):
    """
    Sensor that pokes every `poke_interval` seconds to check if all tasks in the DAG have succeeded.
    If after timeout seconds all tasks have not succeeded, it raises an AirflowException.
    """
 
    @apply_defaults
    def __init__(self, timeout=15, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.timeout = timeout
        self.poke_interval = kwargs.get('poke_interval',10)

    def poke(self, context):
        dag = context['dag']
        ti_list = dag.get_task_instances()

        for ti in ti_list:
            task_id = ti.task_id
            success_key = f"{task_id}_success"
            success_value = context['ti'].xcom_pull(key=success_key, default=False)
            self.log.info(f"{task_id}, success_value:{success_value}")
            if task_id!="email_sender_task" and task_id!="raise_error_task" and task_id!="all_tasks_success_sensor":
                if success_value=="failed!":
                    raise AirflowException(f"{task_id}, success_value:{success_value}")
                if success_value!="success!!":
                    return False
        return True

    def execute(self, context):
        self.log.info(f"Waiting for all tasks in DAG {context['dag'].dag_id} to succeed...")
        super().execute(context)

        start_time = context['ti'].execution_date

        while not self.poke(context): 
            if (context['ti'].execution_date - start_time).total_seconds() > self.timeout:
                #push_msg = {'exit_message_value':'sla_missed'}
                #context['ti'].xcom_push(key='exit_message_key',value = push_msg)
                
                #pull_message1 = context['ti'].xcom_pull(key='exit_message_key')
                #pull_message2 = pull_message1['exit_message_value']
                # Use the message retrieved from XCom
                #task_logger.info(f"100,{pull_message2}")
                raise AirflowException(f"Not all tasks in DAG {context['dag'].dag_id} succeeded within {self.timeout} seconds.")
            self.log.info(f"waiting for tasks in {context['dag'].dag_id}...")
            self.log.info(f"Sleeping for {self.poke_interval} seconds...")
            self._sleep(self.poke_interval)

        self.log.info(f"All tasks in DAG {context['dag'].dag_id} have succeeded.")

all_tasks_success_sensor = AllTasksSuccessSensor(
    task_id='all_tasks_success_sensor',
    timeout= 10800, #3 hours
    poke_interval=10,
    dag=dag1,
)

print_input_task>>datafeed_insider_task>>raise_error_task
all_tasks_success_sensor>>raise_error_task