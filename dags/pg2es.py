from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

from modules.m1_get_data_pg import get_data
from modules.m2_cleaning import cleaning_men
from modules.m3_push_kibana import push_es

default_args= {
    'owner': 'Rafif',
    'start_date': datetime(2022, 11, 30)
}

with DAG(
    "Test_postgres2es",
    description="Let's try",
    schedule_interval="@daily",
    default_args=default_args, 
    catchup=False) as dag:

    fetch_data_pg = PythonOperator(
        task_id='get_pg',
        python_callable=get_data)
    
    cleaning = PythonOperator(
        task_id='transformation',
        python_callable=cleaning_men)
    
    sending_data = PythonOperator(
        task_id='sending_data',
        python_callable=push_es)
    
fetch_data_pg >> cleaning >> sending_data