from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import json
import logging
import requests



dag = DAG(
    'pdill_http_trigger',
    default_args={'retries': 1},
    tags=['example'],
    start_date=datetime(2021, 1, 1),
    catchup=False,
)



def trigger_dag_now(config, dag_id):
    url = f"http://localhost:8080/api/v1/dags/{dag_id}/dagRuns"
    data = json.dumps({"conf": config})
    request = requests.post(
    url,
    data = data,
    headers = {"Content-Type": "application/json",
    "Accept": "application/json",
    "Authorization": "Basic cmlzaGFiOjEyMzQ="})

    logging.info(request.text)
    logging.info(f"--------json--------data----------{data}")




def parse_response(**kwargs):
    json_string = json.loads(kwargs["json_string"])
    dag_id = kwargs["dag_id"]
    for configurations in json_string:
    if configurations["inProgress"] == True:
    logging.info(f"Configurations processing {configurations['configurationId']}")
    continue
    else:
    logging.info(f"Trigger Dag for configuration {configurations['configurationId']}")
    configurations['inProgress'] = str(configurations['inProgress']).lower()
    trigger_dag_now(configurations, dag_id)




task_get_op_response_filter = SimpleHttpOperator(
    http_conn_id='http-pdill-get',
    task_id='get_op_response_filter',
    method='GET',
    endpoint='listjobs',
    dag=dag,
)



task_parse_response = PythonOperator(
    task_id = "parse_response",
    python_callable=parse_response,
    op_kwargs={
    "json_string":"{{ task_instance.xcom_pull(task_ids='get_op_response_filter', key='return_value') }}",
    "dag_id":"dummy_triggered"
    },
    dag=dag
)




task_get_op_response_filter >> task_parse_response