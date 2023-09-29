import pandas as pd
from elasticsearch import Elasticsearch

def push_es():
    #load
    df = pd.read_csv("/opt/airflow/data/data_contoh_clean.csv")

    es = Elasticsearch("http://elasticsearch:9200")

    for i,r in df.iterrows():
        doc=r.to_json()
        res = es.index(index="data_contoh", doc_type="doc", body=doc)
    
    print('Done')