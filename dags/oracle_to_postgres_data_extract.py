from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
import cx_Oracle

# Define default_args dictionary to specify the DAG's default parameters
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Function to extract data from Oracle and load it into a Pandas DataFrame
def extract_data_from_oracle(**kwargs):
    oracle_conn_id = 'oracle_fta_conn'  # Connection ID for Oracle (defined in Airflow Connections)
    oracle_query = "SELECT * FROM the.tenure_application_state_code;"
    
    # Create a SQLAlchemy engine to connect to Oracle
    engine = create_engine(f"oracle+cx_oracle://{oracle_conn_id}")
    
    # Use Pandas to read data from Oracle into a DataFrame
    df = pd.read_sql_query(oracle_query, engine)
    
    # Pass the DataFrame to the next task
    kwargs['ti'].xcom_push(key='oracle_data', value=df)

# Function to load data from Pandas DataFrame into PostgreSQL
def load_data_into_postgres(**kwargs):
    postgres_conn_id = 'postgres_ods_conn'  # Connection ID for PostgreSQL (defined in Airflow Connections)
    
    # Retrieve the DataFrame from the previous task
    df = kwargs['ti'].xcom_pull(key='oracle_data', task_ids='extract_from_oracle')
    
    # Create a SQLAlchemy engine to connect to PostgreSQL
    engine = create_engine(f"postgresql://{postgres_conn_id}")
    
    # Use Pandas to write data from the DataFrame into PostgreSQL
    df.to_sql('tenure_application_state_code', engine,'public', if_exists='replace', index=False)

# Create the DAG
dag = DAG(
    'oracle_to_postgres_data_extract',
    default_args=default_args,
    description='Extract data from Oracle and load into PostgreSQL using Pandas',
    schedule_interval=timedelta(days=1),  # Set the schedule as needed
)

# Define PythonOperators to execute Python functions
extract_task = PythonOperator(
    task_id='extract_from_oracle',
    python_callable=extract_data_from_oracle,
    provide_context=True,  # Pass the context to the function
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_into_postgres',
    python_callable=load_data_into_postgres,
    provide_context=True,  # Pass the context to the function
    dag=dag,
)

# Set the task dependencies
extract_task >> load_task
