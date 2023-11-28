from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def print_secrets():
    from airflow.providers.cncf.kubernetes.secret import Secret
    ods_secrets = Secret("env", None, "ods-database")
    print(ods_secrets)

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
    schedule_interval=timedelta(days=1),  # adjust as needed
)

print_task = PythonOperator(
    task_id='secrets_example',
    python_callable=print_secrets,
    dag=dag,
)
