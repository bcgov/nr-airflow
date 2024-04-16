from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import datetime, timedelta

# Define default_args dictionary
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 11, 27),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['NRM.DataFoundations@gov.bc.ca'],
    'email_on_failure': True,
    'email_on_retry': True,
}

# Define the DAG
dag = DAG(
    'permitting_pipeline_etl_batch_id_update',
    default_args=default_args,
    description='DAG to update etl_batch_id in PostgreSQL',
    schedule_interval=None,  # Set your desired schedule_interval
    catchup=False,  # Set to False to skip historical runs
)

# Define the SQL query
update_sql_query = """
    UPDATE ods_data_management.audit_batch_id
    SET etl_batch_status='success', etl_batch_end_time=current_timestamp 
    WHERE etl_batch_id=(
        SELECT MAX(etl_batch_id)
        FROM ods_data_management.audit_batch_id
        WHERE etl_batch_status='started' AND etl_batch_name='permitting_data_pipeline'
    );
"""

# Define the task using PostgresOperator for the update query
update_task = PostgresOperator(
    task_id='update_etl_batch_id',
    postgres_conn_id='postgres_ods_conn',  # Specify your PostgreSQL connection ID
    sql=update_sql_query,
    autocommit=True,
    dag=dag,
)

if __name__ == "__main__":
    dag.cli()
