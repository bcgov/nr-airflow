from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import datetime
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 23),
    # Add other necessary default arguments
}

with DAG('postgres_example_insert', default_args=default_args, schedule_interval=None) as dag:
    t1 = PostgresOperator(
        task_id='execute_sql',
        sql="INSERT INTO public.tenure_application_state_code SELECT tenure_application_state_code, description,effective_date,expiry_date,update_timestamp FROM fta_replication.tenure_application_state_code",
        postgres_conn_id="postgres_ods_conn",  # Update connection ID
        autocommit=True,
    )

    t1
