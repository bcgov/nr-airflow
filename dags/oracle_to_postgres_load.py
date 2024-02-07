from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


@task
def get_data_from_oracle():
    oracle_hook = OracleHook(oracle_conn_id='oracle_fta_conn')
    data = oracle_hook.get_pandas_df(sql="SELECT TENURE_APPLICATION_STATE_CODE, DESCRIPTION, EFFECTIVE_DATE ,EXPIRY_DATE ,EXPIRY_DATE,UPDATE_TIMESTAMP FROM the.tenure_application_state_code")
    # Convert Timestamp objects to string representation
    data['EFFECTIVE_DATE'] = data['EFFECTIVE_DATE'].dt.strftime('%Y-%m-%d')
    data['EXPIRY_DATE'] = data['EXPIRY_DATE'].dt.strftime('%Y-%m-%d')
    data['UPDATE_TIMESTAMP'] = data['UPDATE_TIMESTAMP'].dt.strftime('%Y-%m-%d %H:%M:%S')
    return data.to_dict(orient='records')

@task
def insert_data_into_postgres(data):
    pg_hook = PostgresHook(postgres_conn_id='postgres_ods_conn')
    pg_hook.insert_rows(table="tenure_application_state_code",rows=data)
        
with DAG ('oracle_to_postgres_load',start_date=datetime(2024, 2, 7)) as dag:
    data = get_data_from_oracle()
    insert_data_into_postgres(data)
