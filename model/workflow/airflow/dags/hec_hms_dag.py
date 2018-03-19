'''
Created on Mar 15, 2018
@author: hasitha
'''

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from model.hec_hms.operators.rf_to_csv_opearator import RfToCsvOperator


# each Workflow/DAG must have a unique text identifier
WORKFLOW_DAG_ID = 'hec_hms_workflow_dag'

# start/end times are datetime objects
WORKFLOW_START_DATE = datetime(2018, 3, 14)

# schedule/retry intervals are timedelta objects
# here we execute the DAGs tasks every day
WORKFLOW_SCHEDULE_INTERVAL = timedelta(1)

# default arguments are applied by default to all tasks 
# in the DAG
WORKFLOW_DEFAULT_ARGS = {
    'owner': 'hasitha',
    'depends_on_past': False,
    'start_date': WORKFLOW_START_DATE,
    'email': ['hasitha.10@cse.mrt.ac.lk'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# initialize the DAG
dag = DAG(
    dag_id=WORKFLOW_DAG_ID,
    start_date=WORKFLOW_START_DATE,
    schedule_interval=WORKFLOW_SCHEDULE_INTERVAL,
    default_args=WORKFLOW_DEFAULT_ARGS,
)


########################
# Instantiating Tasks  #
########################

rf_to_csv_convert = RfToCsvOperator(
    task_id='rf_to_csv_convert',
    dag=dag)

dummy_start = DummyOperator(
    task_id='dummy_start',
    dag=dag)

dummy_start >> rf_to_csv_convert
