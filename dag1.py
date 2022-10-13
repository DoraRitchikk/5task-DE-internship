from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import pandas as pd
import pymongo as d
import json

default_args = {
    'depends_on_past': False,
    'start_date': datetime(2015, 12, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('pythonDag', default_args=default_args)

def getDataFromCsv():
    df = pd.read_csv('C:/Users/Dora Ritchik/Desktop/DE-Internship/task5/tiktok_google_play_reviews.csv', delimiter=',')
    print(df)
 
t1 = PythonOperator(
    task_id='task_1',
    python_callable = getDataFromCsv,
    dag=dag)

dag2 = DAG('pythonDag2', default_args=default_args)

def cleanData():
    df.drop_duplicates()
    df = df.replace('null',"-").bfill()
    df = df.replace(['\+', r'(?u)[^\w\s\?\.\,\-\:\d]+', '\s*$'], ['','',''], regex=True)
    df = df.sort_values(by = 'at')

 
t2 = PythonOperator(
    task_id='task_2',
    python_callable = cleanData,
    dag=dag2)

dag3 = DAG('pythonDag3', default_args=default_args)

def pushData():
    #Подключение к БД и коллекции
    client = d.MongoClient('localhost',27017)
    db = client['task5']
    collection = db['task5']

    record = json.loads(df.T.to_json()).values()
    collection.insert_many(record)

t3 = PythonOperator(
    task_id='task_3',
    python_callable = pushData,
    dag=dag3)

