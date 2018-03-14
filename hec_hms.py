'''
Created on Mar 14, 2018
@author: hasitha
'''

import logging
import glob
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashshOperator

log = logging.getLogger(__name__)

dag = DAG('hello_world', 
          description='Hec-Hms model running.',
          schedule_interval='0 2 * * *',
          start_date=datetime(2018, 3, 14), 
          catchup=False)

# Configuration data in CONFIG.json
config_dict = {'HEC_HMS_DIR': 'hec-hms41','HEC_DSSVUE_DIR'    : 'dssvue','STATUS_FILE': 'Status.txt','OUTPUT_DIR': 'OUTPUT','RAIN_CSV_FILE': 'DailyRain.csv','TIME_INTERVAL': 60,'HEC_HMS_MODEL_DIR': './2008_2_Events','DSS_INPUT_FILE': './2008_2_Events/2008_2_Events_input.dss','DSS_OUTPUT_FILE': './2008_2_Events/2008_2_Events_run.dss','HEC_HMS_CONTROL': './2008_2_Events/Control_1.control','HEC_HMS_RUN': './2008_2_Events/2008_2_Events.run','HEC_HMS_GAGE': './2008_2_Events/2008_2_Events.gage','DISCHARGE_CSV_FILE': 'DailyDischarge.csv','INFLOW_DAT_FILE': './FLO2D/INFLOW.DAT','OUTFLOW_DAT_FILE': './FLO2D/OUTFLOW.DAT','RF_DIR_PATH': '/mnt/disks/wrf-mod/OUTPUT/RF/','KUB_DIR_PATH': '/mnt/disks/wrf-mod/OUTPUT/kelani-upper-basin','RF_GRID_DIR_PATH': '/mnt/disks/wrf-mod/OUTPUT/colombo/','RF_FORECASTED_DAYS': -1,'FLO2D_RAINCELL_DIR_PATH': '/mnt/disks/wrf-mod/OUTPUT/kelani-basin','HOST_ADDRESS': '10.138.0.4','HOST_PORT': 8080,'MYSQL_HOST': '10.138.0.6','MYSQL_USER': 'curw','MYSQL_PASSWORD': 'curw@123','MYSQL_DB': 'curw'}

tmp_config_dict = {'ROOT_DIR': '/home','INIT_DIR': '/home','META_FLO2D_DIR': '/home/META_FLO2D','FLO2D_DIR': '/home/FLO2D','rf_forecasted_date': '2018-03-14'}


def print_hello():
    return 'Load CONFIG.json data'


dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)


hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)