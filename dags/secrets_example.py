from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.secret import Secret
import os

ods_secrets = Secret("env", None, "ods-database")
ods_secrets.to_env_secret()
# dict_secrets = ods_secrets.__dict__ 

def print_secrets():
    print("secret object:", ods_secrets)
    print("env var database name:", os.getenv('ODS_DATABASE'))
    print("index secret:", ods_secrets.secret('ODS_DATABASE'))

dag = DAG(
    dag_id='secrets_example',
    schedule=None,
    start_date=datetime(2023, 11, 23),
    catchup=False,
)

print_task = PythonOperator(
    task_id='secrets_example',
    python_callable=print_secrets,
    provide_context=True,  
    op_args=[ods_secrets],  # Pass the Secret object as an argument
    # env_vars={'ODS_DATABASE': '{{ var.value.ODS_DATABASE }}'},
    dag=dag
)