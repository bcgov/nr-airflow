from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.cncf.kubernetes.secret import Secret
import os

ods_secrets = Secret(deploy_type= "env", deploy_target="env", secret = "ods-database")

def print_secrets():
    print("secret object:", ods_secrets)
    print("env var database name:", os.getenv('DATABASE'))
    print("env var database name:", os.getenv('NA'))

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'secrets_example',
    default_args=default_args,
    description='A simple DAG to print code',
    schedule=None # adjust as needed
)

print_task = PythonOperator(
    task_id='secrets_example',
    python_callable=print_secrets,
    dag=dag,
)
