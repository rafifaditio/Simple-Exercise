import psycopg2 as db
import pandas as pd
from elasticsearch import Elasticsearch
from sqlalchemy import create_engine

def get_data():
    engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres/airflow")
    conn = engine.connect()

    df = pd.read_sql_query("select * from car_assignment", conn)

    #save to .data
    df.to_csv('/opt/airflow/data/data_contoh.csv' , sep=',', index=False)
