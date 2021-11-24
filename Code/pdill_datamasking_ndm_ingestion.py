from datetime import datetime
from airflow import DAG

from utils.pdill_dag_utils import PdilDagUtils
du = PdilDagUtils()



default_args = {
    'description':'TDMF data masking',
    'start_date': datetime(2020,9,25),
    'email': ['prashanth.desani@wellsfargo.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'on_success_callback': du.notify_email,
    'on_failure_callback': du.notify_email,
}

dag = DAG('pdill_datamasking_ndm_ingestion2', schedule_interval=None, catchup=False,default_args=default_args)
du.dag = dag

configuration_id = ""
pdill_datamasking='pdill_datamasking'

datamasking_pipeline_execution = du.get_configuration_simple_http_operator('datamasking_pipeline_execution', 'pdill_datamasking','/datamasking/execution')
get_configurationId = du.resolve_config_python_operator('get_configurationId')

datamasking_extraction = du.pdill_simple_http_operator('datamasking_extraction', pdill_datamasking, 'get_configurationId', '/datamasking/extraction')
datamasking_extraction_jobstatus = du.pdill_http_sensor('datamasking_extraction_jobstatus', pdill_datamasking, '/datamasking/jobstatus', 'get_configurationId', 5)

datamasking_execution = du.pdill_simple_http_operator('datamasking_execution', pdill_datamasking, 'get_configurationId', '/datamasking/execution')
datamasking_execution_jobstatus = du.pdill_http_sensor('datamasking_execution_jobstatus', pdill_datamasking, '/datamasking/jobstatus', 'get_configurationId', 5)

datamasking_validation = du.pdill_simple_http_operator('datamasking_validation', pdill_datamasking, 'get_configurationId', '/datamasking/validation')
datamasking_validation_jobstatus = du.pdill_http_sensor('datamasking_validation_jobstatus', pdill_datamasking, '/datamasking/jobstatus', 'get_configurationId', 5)

datamasking_review = du.pdill_simple_http_operator('datamasking_review', pdill_datamasking, 'get_configurationId', '/datamasking/review')
datamasking_review_jobstatus = du.pdill_http_sensor('datamasking_review_jobstatus', pdill_datamasking, '/datamasking/jobstatus', 'get_configurationId', 5)

datamasking_ndm = du.pdill_simple_http_operator('datamasking_ndm', pdill_datamasking, 'get_configurationId', '/datamasking/ndm')
datamasking_ndm_jobstatus = du.pdill_http_sensor('datamasking_ndm_jobstatus', pdill_datamasking, '/datamasking/jobstatus', 'get_configurationId', 5)

datamasking_ingestion = du.pdill_simple_http_operator('datamasking_ingestion', pdill_datamasking, 'get_configurationId', '/datamasking/ingestion')

datamasking_complete = du.pdill_simple_http_operator('datamasking_complete', pdill_datamasking, 'get_configurationId', '/datamasking/complete')


datamasking_pipeline_execution >> get_configurationId >> datamasking_extraction >> datamasking_extraction_jobstatus >> datamasking_execution >> datamasking_execution_jobstatus >> datamasking_validation >> datamasking_validation_jobstatus >> datamasking_review >> datamasking_review_jobstatus >> datamasking_ndm >> datamasking_ndm_jobstatus >> datamasking_ingestion >> datamasking_complete

