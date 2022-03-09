from datetime import datetime
from airflow import DAG
import json

# Imports for v1.10.10
# from airflow.operators.http_operator import SimpleHttpOperator
# from airflow.sensors.http_sensor import HttpSensor

# Imports for v2.1.0
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor

# Common imports
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
from airflow import AirflowException

def notify_email(kwargs):
    """Send custom email alerts."""
    ti = kwargs['ti']
    dag_run = kwargs['dag_run']
    var = kwargs['var']['json']
    params = kwargs['params']
    recipient_emails=['prashanth.desani@wellsfargo.com']


    logs_link = '{}/log?task_id={}&dag_id={}&execution_date={}'.format(dag_run.conf.get("webserver", "base_url"), ti.task_id, ti.dag_id, ti.execution_date)


    title = ''
    body = ''

    # email title.
    if dag_run._state == "success":
        title = "[ Airflow Success Alert  ]: {dag_id} Execution Completed".format(dag_id=dag_run.dag_id)
        body = """
            Hi Everyone, <br>
            <br>
            Job {dag_id} has executed successfully.<br>
            <br>
            Thank you,<br>
            Airflow Bot <br>
            """.format(dag_id=dag_run.dag_id)
    else:
        if ti.state == "failed":
            title = "[ Airflow Failure Alert ]: {task_id} Failed for {dag_id} failed".format(task_id=ti.task_id, dag_id=dag_run.dag_id)
            body = """
            Hi Everyone, <br>
            <br>
            Task {task_id} failed.<br>
            Please check the log at : {log_link}<br>
            <br>
            Thank you,<br>
            Airflow bot <br>
            """.format(task_id=ti.task_id,log_link=ti.log_url )
        elif ti.state == "success":
            title = "[ Airflow Success Alert ]: {task_id} Succeeded for {dag_id}".format(task_id=ti.task_id, dag_id=dag_run.dag_id)
            body = """
            Hi Everyone, <br>
            <br>
            Task {task_id} has completed successfully<br>
            <br>
            Thank you,<br>
            Airflow bot <br>
            """.format(task_id=ti.task_id,log_link=ti.log_url )
        else: 
            raise AirflowException('{} task state is not supported in email notifications'.format(ti.state))

    send_email(recipient_emails, title, body)

# Configuration ID will be passed to this dag through configuration
def resolve_config_id(**kwargs):
    # task_instance = kwargs['task_instance']
    # configuration_dict = json.loads(task_instance.xcom_pull(task_ids='datamasking_pipeline_execution', key='return_value'))
    # configuration_id = configuration_dict['configurationId']
    dag_run = kwargs['dag_run']
    configuration_id = dag_run['configurationId']
    return configuration_id

# Set the connection endpoint as per your environment
pdill_datamasking_conn_id='pdill_datamasking'
# Change to actual endpoint for Endpoint
ingestion_endpoint="/sample/endpoint"

default_args = {
    'description':'TDMF data masking',
    'start_date': datetime(2020,9,25),
    'email': ['prashanth.desani@wellsfargo.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'on_success_callback': notify_email,
    'on_failure_callback': notify_email,
}

dag = DAG('pdill_datamasking_ndm_ingestion_only', schedule_interval=None, catchup=False,default_args=default_args)

op_get_configurationId = PythonOperator(
    task_id='get_configurationId',
    dag=dag,
    python_callable=resolve_config_id,
)

op_pdill_ingestion=SimpleHttpOperator(
    task_id='pdill_ingestion',
    http_conn_id=pdill_datamasking_conn_id,
    endpoint=ingestion_endpoint,
    headers={"Content-Type": "application/json"},
    data="{{ dag_run.conf['data'] | tojson }}",
    response_check=lambda response: response.json()['configurationId'] != None,
    log_response=True,
    dag=dag
)

op_pdill_ingestion_jobstatus = HttpSensor(
    task_id='pdill_ingestion_jobstatus',
    http_conn_id=pdill_datamasking,
    endpoint="/datamasking/jobstatus/{{ task_instance.xcom_pull(task_ids='get_configurationId', key='return_value') }}",
    method='GET',
    request_params={},
    response_check=lambda body:monitor_job(body),
    #response_check=lambda response: response.json()['status']=="SUCCESS",
    poke_interval=5,
    dag=dag
)

op_get_configurationId >> op_pdill_ingestion >> op_pdill_ingestion_jobstatus