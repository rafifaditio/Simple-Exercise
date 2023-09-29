from airflow.models import DAG

from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

from datetime import datetime, timedelta

from elasticsearch import Elasticsearch, helpers

def get_connection():
    es = Elasticsearch("http://elasticsearch:9200")
    print(es.ping())


def send_data():
    es = Elasticsearch("http://elasticsearch:9200")
    doc = {
        "name": 'testing_0',
        "age": 27
    }
    res = es.index(index="trial", doc_type="_doc", body=doc)
    print(res)


default_args= {
    'owner': 'Rafif',
    'start_date': datetime(2023, 9, 25)
}

with DAG(
    "Test_es",
    description="Let's try",
    schedule_interval=timedelta(minutes=5),
    default_args=default_args, 
    catchup=False) as dag:

    try_connection = PythonOperator(
        task_id='try',
        python_callable=get_connection)
    
    print_starting = BashOperator(task_id='starting',
        bash_command='echo "Starting task....."')
    
    sending_data = PythonOperator(
        task_id='sending_data',
        python_callable=send_data)
    
    print_ending = BashOperator(task_id='ending',
        bash_command='echo "done"')
    
print_starting >> try_connection >> sending_data >> print_ending