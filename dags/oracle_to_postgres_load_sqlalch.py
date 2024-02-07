from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.oracle_hook import OracleHook


def extract_from_oracle(sql_query):
    # Create Oracle hook
    oracle_hook = OracleHook(oracle_conn_id='oracle_fta_conn')
    
    # Create SQLAlchemy engine
    engine = oracle_hook.get_sqlalchemy_engine()
    
    # Execute custom SQL query
    with engine.connect() as connection:
        result = connection.execute(sql_query)
        data = result.fetchall()
    
    return data


def load_to_postgres(data):
    # Create PostgreSQL hook
    postgres_hook = PostgresHook(postgres_conn_id='postgres_ods_conn')
    
    # Load data into PostgreSQL
    postgres_hook.insert_rows(table="public.tenure_application_state_code", rows=data)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('oracle_to_postgres_load_sqlalch', default_args=default_args, schedule_interval=None) as dag:
    extract_data = PythonOperator(
        task_id='extract_data_from_oracle',
        python_callable=extract_from_oracle,
        op_kwargs={'sql_query': "SELECT TENURE_APPLICATION_STATE_CODE, DESCRIPTION, EFFECTIVE_DATE ,EXPIRY_DATE ,EXPIRY_DATE,UPDATE_TIMESTAMP FROM the.tenure_application_state_code"}
    )

    load_data = PythonOperator(
        task_id='load_data_to_postgres',
        python_callable=load_to_postgres,
        op_args=[extract_data.output]
    )

    extract_data >> load_data
