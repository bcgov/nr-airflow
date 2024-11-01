from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import psycopg2
import logging

default_args = {
    'owner': 'Data Foundations',
    'depends_on_past': False,
    'email': ['NRM.DataFoundations@gov.bc.ca'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='collect_schema_sizes_dag',
    start_date=datetime(2024, 11, 1),
    max_active_runs=3,
    schedule_interval='0 8 1 * *',  # At 08:00 on Fist day of the month   
    default_args=default_args,
    template_searchpath="/opt/bitnami/airflow/dags/git_nr-airflow-dags/include/",
    catchup=False,
    description='DAG to collect PostgreSQL schema sizes monthly'
) as dag:

  gather_monthly_sizes = PostgresOperator(
       task_id='execute_schema_size_function',
       sql="'SELECT ods_data_management.collect_schema_sizes();'",
       postgres_conn_id="postgres_ods_conn",
       autocommit=True,
   )

  gather_monthly_sizes 