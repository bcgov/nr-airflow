from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import datetime
from airflow.operators.python_operator import PythonOperator

@dag(
    "demo_silver_database_connection",
    start_date=datetime(2024, 1, 16),
    max_active_runs=3,
    schedule="None",
    template_searchpath="/opt/bitnami/airflow/dags/git_nr-airflow-dags/include/",
    catchup=False
)
def exe_qry():
  
    select_all = PostgresOperator(
        task_id='execute_sql',
        sql="SELECT * FROM public.example_table",
        postgres_conn_id="postgres_silver_tsc_connection",  
        autocommit=True
    )
    select_all

exe_qry()
