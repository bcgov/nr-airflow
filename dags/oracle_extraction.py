from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import oracledb
import psycopg2
import configparser

# Work in progress

# Function to retrieve Oracle database connection
t1 = OracleOperator(
        task_id='execute_sql',
        sql="SELECT count(*) FROM THE.TENURE_APPLICATION_STATE_CODE",
        oracle_conn_id="oracle_fta_dev_conn",
        autocommit=True,
    )

# Function to execute SQL queries
def execute_sql(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    connection.close()
    return rows

# Function to read SQL query from a file
def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        query = file.read()
    return query

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'oracle_extraction',
    default_args=default_args,
    description='Your DAG description',
    schedule_interval=timedelta(days=1),  # Adjust the schedule interval as needed
)

# Task to retrieve data from Oracle database
oracle_task = PythonOperator(
    task_id='retrieve_data_from_oracle',
    python_callable=execute_sql,
    dag=dag,
)

# Set task dependencies
oracle_task
