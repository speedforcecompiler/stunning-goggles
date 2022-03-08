from datetime import datetime
from airflow import DAG
import json
from airflow.models import taskinstance

# Imports for v1.10.10
# from airflow.operators.http_operator import SimpleHttpOperator
# from airflow.sensors.http_sensor import HttpSensor

# Imports for v2.1.0
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor

# Common imports
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow import AirflowException

default_args = {
    'description':'Snowflake POC',
    'start_date': datetime(2020,9,25),
    'email': ['airflow@airflow.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = DAG('pdill_datamasking_ndm_ingestion', schedule_interval=None, catchup=False,default_args=default_args)

def start_batch():
    print("Batch Started")
    print("I will return batch_ids for initialization")
    return 2222

def get_process_name(**kwargs):
    task_instance = kwargs['task_instance']
    print("Initializing Process ID")
    process_dict = {
        "2222":{
            "id":"101",
            "process_name":"SRD_DAAS_STG_REF_CLIENT_AGENCIES"
        }
    }
    process = process_dict.get(task_instance.xcom_pull(task_ids='start_batch', key='return_value'))
    return process['process_name']

def get_process_id(**kwargs):
    task_instance = kwargs['task_instance']
    print("Initializing Process ID")
    process_dict = {
        "2222":{
            "id":"101",
            "process_name":"SRD_DAAS_STG_REF_CLIENT_AGENCIES"
        }
    }
    process = process_dict.get(task_instance.xcom_pull(task_ids='start_batch', key='return_value'))
    return process['id']

def get_batch_run_id():
    print("I will query snowflake and return batch run id")
    return 101

def get_process_run_id():
    print("I will query snowflake and get the process run id")
    return 222
    
start_batch_op = PythonOperator(
    task_id = 'start_batch',
    python_callable=start_batch,
    dag=dag
)

batch_initialization_op = BashOperator(
    task_id='batch_initialization',
    bash_command='echo "call BATCH_INIT({{ task_instance.xcom_pull(task_ids=\'start_batch\', key=\'return_value\') }})"',
    dag=dag
)

get_batch_run_id_op=PythonOperator(
    task_id="get_batch_run_id",
    python_callable=get_batch_run_id,
    dag=dag
)

get_process_name_op = PythonOperator(
    task_id='get_process_name',
    python_callable=get_process_name,
    dag=dag
)

get_process_id_op = PythonOperator(
    task_id='get_process_id',
    python_callable=get_process_id,
    dag=dag
)

process_init_op = BashOperator(
    task_id="process_initalization",
    str='echo "PROCESS_INIT({{ task_instance.xcom_pull(task_ids=\'get_process_name\', key=\'return_value\')}})',
    dag=dag
)

get_process_run_id_op=PythonOperator(
    task_id="get_process_run_id",
    python_callable=get_process_run_id,
    dag=dag
)

stored_proc_start_op=BashOperator(
    task_id='{{ task_instance.xcom_pull(task_ids="get_process_name", key="return_value") }}',
    str='echo "call {{ task_instance.xcom_pull(task_ids=\'get_process_name\', key=\'return_value\')}}({{ task_instance.xcom_pull(task_ids=\'get_process_id\', key=\'return_value\') }});',
    dag=dag
)

process_update_op=BashOperator(
    task_id="process_update",
    str='echo "call PROCESS_UPDATE({{ task_instance.xcom_pull(task_ids=\'get_process_run_id\', key=\'return_value\') }});"',
    dag=dag
)

batch_update_op=BashOperator(
    task_id="batch_update",
    str='echo "call BATCH_UPDATE({{ task_instance.xcom_pull(task_ids=\'get_batch_run_id\', key=\'return_value\') }})"'
)



start_batch_op >> batch_initialization_op >> get_batch_run_id_op >> get_process_name_op >> get_process_id_op >> process_init_op >> get_process_run_id_op >> stored_proc_start_op >> process_update_op >> batch_update_op