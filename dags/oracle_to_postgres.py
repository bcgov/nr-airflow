from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'oracle_to_postgres',
    default_args=default_args,
    description='DAG to extract data from Oracle and load it into PostgreSQL',
    schedule_interval='@daily',
)

def extract_from_oracle(**kwargs):
    oracle_conn_id = kwargs.get('oracle_conn_id', 'oracle_default')
    oracle_hook = OracleHook(oracle_conn_id)
    result = oracle_hook.get_pandas_df("SELECT * FROM the.tenure_application_state_code")
    return result

def load_to_postgres(**kwargs):
    postgres_conn_id = kwargs.get('postgres_conn_id', 'postgres_default')
    pg_hook = PostgresHook(postgres_conn_id)
    ti = kwargs.get('ti')
    df = ti.xcom_pull(task_ids='extract_from_oracle_task')
    pg_hook.insert_rows(table='tenure_application_state_code', rows=df.to_dict(orient='records'))

with dag:
    extract_from_oracle_task = PythonOperator(
        task_id='extract_from_oracle_task',
        python_callable=extract_from_oracle,
        provide_context=True,
        op_kwargs={'oracle_conn_id': 'oracle_fta_conn'},
    )

    load_to_postgres_task = PythonOperator(
        task_id='load_to_postgres_task',
        python_callable=load_to_postgres,
        provide_context=True,
        op_kwargs={'postgres_conn_id': 'postgres_ods_conn'},
    )

    extract_from_oracle_task >> load_to_postgres_task
