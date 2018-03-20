'''
Created on Mar 15, 2018
@author: hasitha
'''

from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import logging

# initialize the DAG
log = logging.getLogger(__name__)

dag = DAG('hec_hms',
          description='Hec-Hms model running.',
          schedule_interval='0 2 * * *',
          start_date=datetime(2018, 3, 20),
          catchup=False)


# test methods
def rf_to_csv_convert():
    print('----------------------------Rf to csv convert------------------------------')


def csv_to_dss():
    print('----------------------------Convert csv to dss-----------------------------')


def update_hec_hms_model():
    print('--------------------------Update Hec-Hms model-----------------------------')


def run_hec_hms_model():
    print('------------------------Run Hec-Hms model----------------------------------')


########################
# Instantiating Tasks  #
########################

csv_to_dss = PythonOperator(
    task_id='csv_to_dss',
    python_callable=csv_to_dss(),
    dag=dag)

update_hec_hms_model = PythonOperator(
    task_id='update_hec_hms_model',
    python_callable=update_hec_hms_model(),
    dag=dag)

run_hec_hms_model = PythonOperator(
    task_id='run_hec_hms_model',
    python_callable=run_hec_hms_model(),
    dag=dag)

rf_to_csv_convert = PythonOperator(
    task_id='rf_to_csv_convert',
    python_callable=rf_to_csv_convert(),
    dag=dag)

rf_to_csv_convert >> csv_to_dss >> update_hec_hms_model >> run_hec_hms_model
