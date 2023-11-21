from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import oracledb
import psycopg2
import configparser

# Function to retrieve Oracle database connection
def get_oracle_connection():
    oracle_username = 'APP_NRM_HOUSING'
    oracle_password = 'Welc0me2'
    oracle_host = 'nrkdb03.bcgov'
    oracle_port = '1521'
    oracle_database = 'geoprd.nrs.bcgov'
    
    dsn = oracledb.makedsn(host=oracle_host, port=oracle_port, service_name=oracle_database)
    oracle_connection = oracledb.connect(user=oracle_username, password=oracle_password, dsn=dsn)
    
    return oracle_connection

# Function to retrieve Postgres database connection
def get_postgres_connection():
    postgres_username = 'ods_admin_user'
    postgres_password = 'Y7ss#by641'
    postgres_host = 'theory.bcgov'
    postgres_port = '5433' 
    postgres_database = 'odsdev'
    
    postgres_connection = psycopg2.connect(user=postgres_username, password=postgres_password,
                                           host=postgres_host, port=postgres_port, database=postgres_database)
    
    return postgres_connection

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

# Function to insert data into Postgres table
def insert_into_postgres(rows):
    postgres_connection = get_postgres_connection()
    postgres_cursor = postgres_connection.cursor()

    # Your insert logic here
    
    postgres_connection.commit()
    postgres_cursor.close()
    postgres_connection.close()

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
    op_args=[get_oracle_connection(), "select * from ats.ats_managing_fcbc_regions"],
    dag=dag,
)

# Task to insert data into Postgres table
postgres_task = PythonOperator(
    task_id='insert_into_postgres',
    python_callable=insert_into_postgres,
    provide_context=True,  # Pass the context to the Python callable
    op_args=[],
    dag=dag,
)

# Set task dependencies
oracle_task
