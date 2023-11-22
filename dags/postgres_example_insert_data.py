from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 23),
    # Add other necessary default arguments
}

with DAG('postgres_data_transfer', default_args=default_args, schedule_interval=None) as dag:
    t1 = PostgresOperator(
        task_id='read_data',
        sql="SELECT tenure_application_state_code, description,effective_date,expiry_date,update_timestamp FROM fta_replication.tenure_application_state_code",
        postgres_conn_id="postgres_ods_conn",
        autocommit=True,
    )

    t2 = PostgresOperator(
        task_id='insert_data',
        sql="INSERT INTO public.tenure_application_state_code (tenure_application_state_code, description,effective_date,expiry_date,update_timestamp) VALUES (%s, %s, %s, %s, %s)",
        postgres_conn_id="postgres_ods_conn",
        autocommit=True,
        params="{{ task_instance.xcom_pull(task_ids='read_data') }}",  # Use XCom to get the result
    )

    t1 >> t2
