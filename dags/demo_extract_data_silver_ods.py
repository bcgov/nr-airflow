from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import requests
import json
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 16),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'extract_data_from_silver_to_ods_postgres',
    default_args=default_args,
    description='A DAG to extract data from a REST API and load it into PostgreSQL',
    schedule_interval='@daily',
)

def extract_rest_api_data():
    api_url = "https://postrest1-c2b678-dev.apps.silver.devops.gov.bc.ca/sample_table"
    response = requests.get(api_url)
    data = response.json()
    return data

def load_data_to_postgres(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_ods_conn') # Make sure you have defined this connection in Airflow
    data = kwargs['ti'].xcom_pull(task_ids='extract_rest_api_data')
    for row in data:
        pg_hook.run("INSERT INTO public.sample_table (id, name, age, email) VALUES (%s, %s,%s ,%s)", parameters=(row['id'], row['name'], row['age'], row['email']))

extract_task = PythonOperator(
    task_id='extract_rest_api_data',
    python_callable=extract_rest_api_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data_to_postgres',
    python_callable=load_data_to_postgres,
    provide_context=True,
    dag=dag,
)

extract_task >> load_task
