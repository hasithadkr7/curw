'''
Created on Mar 14, 2018
@author: hasitha
'''

import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
#from hec_hms.operators import rf_to_csv as rf_to_csv_converter
from hec_hms.operators import rf_to_csv_opearator as RfToCsvOperator

log = logging.getLogger(__name__)

dag = DAG('hec_hms',
          description='Hec-Hms model running.',
          schedule_interval='0 2 * * *',
          start_date=datetime(2018, 3, 14),
          catchup=False)


def csv_to_dss():
    return 'Convert csv to dss'


def update_hec_hms_model():
    return 'Update Hec-Hms model'


def run_hec_hms_model():
    return 'Run Hec-Hms model'


# rft_to_csv_operator = PythonOperator(task_id='rft_to_csv',
#                                      python_callable=rf_to_csv_converter.rf_to_csv_convert(),
#                                      dag=dag)

csv_to_dss_operator = PythonOperator(task_id='csv_to_dss',
                                     python_callable=csv_to_dss,
                                     dag=dag)

update_hec_hms_model_operator = PythonOperator(task_id='update_hec_hms_model',
                                               python_callable=update_hec_hms_model,
                                               dag=dag)

run_hec_hms_model_operator = PythonOperator(task_id='run_hec_hms_model',
                                            python_callable=run_hec_hms_model,
                                            dag=dag)

rft_to_csv_operator = RfToCsvOperator(task_id='rft_to_csv',

                                      dag=dag)

rft_to_csv_operator >> csv_to_dss_operator >> update_hec_hms_model_operator >> run_hec_hms_model_operator
