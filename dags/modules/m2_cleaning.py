import pandas as pd

def cleaning_men():
    #load
    df = pd.read_csv("/opt/airflow/data/data_contoh.csv")

    df.dropna(inplace=True)
    df = df.sample(100)

    #save to .data as data_clean
    df.to_csv('/opt/airflow/data/data_contoh_clean.csv' , sep=',', index=False)
