from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime

# Define default_args dictionary
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 11, 23),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'permitting_pipeline_etl_batch_id_creation',
    default_args=default_args,
    description='DAG to insert a etl_batch_id record into a ODS table',
    schedule_interval=None,  # Set your desired schedule_interval
    catchup=False,  # Set to False to skip historical runs
)

current_date = datetime.now().strftime('%Y-%m-%d')
current_datetime = datetime.now()

def check_record_existence():

    # PostgreSQL connection ID configured in Airflow
    postgres_conn_id = 'postgres_ods_conn'

    # SQL query to check if the record with the given batch_id exists
    select_query = f"""SELECT COUNT(*) FROM app_rrs1.audit_batch_id WHERE etl_batch_id = '{current_date}';"""

    try:
        # Use PostgresHook to connect to PostgreSQL
        postgres_hook = PostgresHook(postgres_conn_id)
        result = postgres_hook.get_first(select_query)

        # If the count is greater than 0, the record exists
        record_exists = result[0] > 0

        return 'skip_insert' if record_exists else 'insert_into_postgres'

    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return 'insert_into_postgres'  # Assume insertion in case of an error


# Define the SQL statement for insertion
insert_sql = f"""
    INSERT INTO app_rrs1.audit_batch_id (etl_batch_id, etl_batch_name, etl_batch_status,etl_batch_start_time,etl_batch_end_time) 
    VALUES ('{current_date}', 'permitting_data_pipeline', 'started','{current_datetime}',null);
"""

# Define the task to check record existence
check_existence_task = PythonOperator(
    task_id='check_record_existence',
    python_callable=check_record_existence,
    provide_context=True,
    dag=dag,
)

# Define the branching task
branch_task = BranchPythonOperator(
    task_id='branch_task',
    python_callable=check_record_existence,
    provide_context=True,
    dag=dag,
)

# Define the task to skip the insertion
skip_insert_task = PythonOperator(
    task_id='skip_insert',
    python_callable=lambda **kwargs: print("Skipping insertion task."),
    dag=dag,
)

# Define the task using PostgresOperator for insertion
insert_task = PostgresOperator(
    task_id='insert_into_postgres',
    postgres_conn_id='postgres_ods_conn',  # Specify your PostgreSQL connection ID
    sql=insert_sql,
    autocommit=True,
    dag=dag,
)

# Set up task dependencies
check_existence_task >> branch_task
branch_task >> [skip_insert_task, insert_task]

if __name__ == "__main__":
    dag.cli()
