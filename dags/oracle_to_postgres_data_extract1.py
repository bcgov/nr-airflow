from airflow import DAG
from airflow.providers.oracle.operators.oracle import OracleOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import datetime, timedelta
import pandas as pd

# Define default_args dictionary to specify the DAG's default parameters
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Oracle SQL query to extract data
oracle_query = "SELECT * FROM the.tenure_application_state_code;"

# Postgres SQL query to load data
postgres_query = "INSERT INTO public.tenure_application_state_code VALUES (%s, %s,%s,%s,%s);"  # Replace with actual query

# Create the DAG
dag = DAG(
    'oracle_to_postgres_data_extract1',
    default_args=default_args,
    description='Extract data from Oracle and load into PostgreSQL using Operators and Pandas',
    schedule_interval=timedelta(days=1),  # Set the schedule as needed
)

# Define OracleOperator to execute the Oracle SQL query
oracle_task = OracleOperator(
    task_id='extract_from_oracle',
    sql=oracle_query,
    oracle_conn_id='oracle_fta_conn',  # Connection ID for Oracle (defined in Airflow Connections)
    autocommit=True,
    fetchall=True,
    dag=dag,
)

# Define PythonOperator to transform the query result to a Pandas DataFrame
def transform_to_dataframe(**kwargs):
    ti = kwargs['ti']
    query_result = ti.xcom_pull(task_ids='extract_from_oracle')
    df = pd.DataFrame(query_result, columns=["tenure_application_state_code", "description","effective_date","expiry_date","update_timestamp"])  # Replace with actual column names
    ti.xcom_push(key='oracle_data', value=df)

transform_task = PythonOperator(
    task_id='transform_to_dataframe',
    python_callable=transform_to_dataframe,
    provide_context=True,
    dag=dag,
)

# Define PostgresOperator to execute the PostgreSQL SQL query
postgres_task = PostgresOperator(
    task_id='load_into_postgres',
    sql=postgres_query,
    postgres_conn_id='postgres_ods_conn',  # Connection ID for PostgreSQL (defined in Airflow Connections)
    autocommit=True,
    parameters="{{ ti.xcom_pull(task_ids='transform_to_dataframe')[0].values.tolist() }}",
    dag=dag,
)

# Set the task dependencies
oracle_task >> transform_task >> postgres_task
