from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


@task
def get_data_from_oracle():
    oracle_hook = OracleHook(oracle_conn_id='oracle_fta_conn')
    data = oracle_hook.get_pandas_df(sql="SELECT tenure_application_state_code,description,effective_date,expiry_date,null FROM the.tenure_application_state_code")
    return data.to_dict()

@task
def insert_data_into_postgres(data):
    pg_hook = PostgresHook(postgres_conn_id='postgres_ods_conn')
    pg_hook.insert_rows(table="tenure_application_state_code",rows=data)
        
with DAG ('oracle_to_postgres_load',start_date=datetime(2024, 2, 5)) as dag:
    data = get_data_from_oracle()
    insert_data_into_postgres(data)