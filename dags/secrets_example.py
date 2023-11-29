from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.secret import Secret
import os

ods_secrets = Secret(deploy_target="env", deploy_type = "env", secret = "ods-database", key = "ODS_DATABASE")
secret_value = ods_secrets.__eq__()

def print_secrets():
    print("secret object:", ods_secrets)
    print("env var database name:", os.getenv('ODS_DATABASE'))
    print("dictionary secret:", secret_value)

dag = DAG(
    dag_id='secrets_example',
    schedule=None,
    start_date=datetime(2023, 11, 23),
    catchup=False,
)

print_task = PythonOperator(
    task_id='secrets_example',
    python_callable=print_secrets,
    # provide_context=True,  
    # op_args=[ods_secrets],  
    # env_vars={'ODS_DATABASE': '{{ var.value.ODS_DATABASE }}'},
    dag=dag
)