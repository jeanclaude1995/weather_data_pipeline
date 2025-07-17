from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.append('/usr/local/airflow/scripts')

from weather import fetch_and_store

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('weather_data_pipeline',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    ingest_weather = PythonOperator(
        task_id='fetch_and_store_weather',
        python_callable=fetch_and_store
    )

    ingest_weather
