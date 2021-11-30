import json

"""
    Imports for all the operators and the requirements that are used in the dag
"""

from airflow import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.email import send_email

"""
    This is a class inside utils. This class holds all the operators generator functions.
    It also holds other functions that are needed by the operators such as the monitor_job ans notify_email.
    This class is called in the pdill_datamasking_ndm_ingestion.py and there values are passed to the operator generator
    and then the generator returns the operator object that the dag uses to create a task.
    
    Advantages of Operator Factory:
        Quicker Dag rendering
        Change management is easy. If any change is needed you can just go ahead and change it in one place 
        and it will reflect everywhere
"""
class PdilDagUtils:

    """
        This is the constructor that initializes the class with the functions and other variables
    """

    def __init__(self):
        self.dag = ""


    """
        This function is called as a callback from the HttpSensor. Check in the Sensor code below you will see
        there is a 'response_check' option that takes a callable as parameter. In this code we have given the
        'monitor_job' as the callable.

        response : This variable holds the response received from the API
    """
    def monitor_job(response):
        print('###################################monitor process response ##################')
        print(response.text)
        responseJson = json.loads(response.text)
        jobStatus = responseJson["status"]
        if jobStatus == "FAILED":
            raise AirflowException("Job FAILED.")
        else:
            return jobStatus == "SUCCESS"

    def notify_email(kwargs):
        """Send custom email alerts."""
        ti = kwargs['ti']
        dag_run = kwargs['dag_run']
        var = kwargs['var']['json']
        params = kwargs['params']
        recipient_emails = ['prashanth.desani@wellsfargo.com']

        logs_link = '{}/log?task_id={}&dag_id={}&execution_date={}'.format(dag_run.conf.get("webserver", "base_url"),
                                                                           ti.task_id, ti.dag_id, ti.execution_date)

        title = ''
        body = ''

        # email title.
        if dag_run._state == "success":
            title = f"[ Airflow Success Alert  ]: {dag_run.dag_id} Execution Completed"
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
                title = f"[ Airflow Failure Alert ]: {ti.task_id} Failed for {dag_run.dag_id} failed"
                body = """
                Hi Everyone, <br>
                <br>
                Task {task_id} failed.<br>
                Please check the log at : {log_link}<br>
                <br>
                Thank you,<br>
                Airflow bot <br>
                """.format(task_id=ti.task_id, log_link=ti.log_url)
            elif ti.state == "success":
                title = f"[ Airflow Success Alert ]: {ti.task_id} Succeeded for {dag_run.dag_id}"
                body = """
                Hi Everyone, <br>
                <br>
                Task {task_id} has completed successfully<br>
                <br>
                Thank you,<br>
                Airflow bot <br>
                """.format(task_id=ti.task_id, log_link=ti.log_url)
            else:
                raise AirflowException('{} task state is not supported in email notifications'.format(ti.state))

        send_email(recipient_emails, title, body)

    def pdill_simple_http_operator(self, task_id, http_conn_id, config_task, endpoint):
        return SimpleHttpOperator(
            task_id=task_id,
            http_conn_id=http_conn_id,
            endpoint=endpoint+"/{{ task_instance.xcom_pull(task_ids='"+config_task+"', key='return_value') }}",
            method='GET',
            response_check=lambda response: response.json()['status']=="SUCCESS",
            log_response=True,
            dag=self.dag
        )

    def get_configuration_simple_http_operator(self, task_id,  http_conn_id, endpoint):
        return SimpleHttpOperator(
            task_id=task_id,
            http_conn_id=http_conn_id,
            endpoint=endpoint,
            headers={"Content-Type": "application/json"},
            data="{{ dag_run.conf['data'] | tojson }}",
            response_check=lambda response: response.json()['configurationId'] != None,
            xcom_push=True,
            log_response=True,
            dag=self.dag
        )

    def pdill_http_sensor(self, task_id, http_conn_id, endpoint, config_task, poke_interval):
        return HttpSensor(
            task_id=task_id,
            http_conn_id=http_conn_id,
            endpoint=endpoint+"/{{ task_instance.xcom_pull(task_ids='"+config_task+"', key='return_value') }}",
            method='GET',
            request_params={},
            response_check=lambda body:self.monitor_job(body),
            #response_check=lambda response: response.json()['status']=="SUCCESS",
            poke_interval=poke_interval,
            dag=self.dag
        )

    def resolve_config_id(**kwargs):
        task_instance = kwargs['task_instance']
        configuration_dict = json.loads(task_instance.xcom_pull(task_ids='datamasking_pipeline_execution', key='return_value'))
        configuration_id = configuration_dict['configurationId']
        return configuration_id

    def resolve_config_python_operator(self, task_id, dag_obj):
        return PythonOperator(
            task_id=task_id,
            dag=self.dag,
            python_callable=self.resolve_config_id,
            provide_context=True
        )

    pass