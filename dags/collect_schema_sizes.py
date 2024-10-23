from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import psycopg2
import logging

default_args = {
    'owner': 'Data Foundations',
    'depends_on_past': False,
    'email': ['NRM.DataFoundations@gov.bc.ca'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def collect_schema_sizes():
    # Set up logging
    logger = logging.getLogger("airflow.task")

    # Retrieve PostgreSQL connection parameters from Airflow variables
    postgres_host = Variable.get('ODS_HOST')
    postgres_port = Variable.get('ODS_PORT')
    postgres_dbname = Variable.get('ODS_DATABASE')
    postgres_user = Variable.get('ODS_USERNAME')
    postgres_password = Variable.get('ODS_PASSWORD')

    logger.info("Connecting to PostgreSQL database")

    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=postgres_host,
            port=postgres_port,
            dbname=postgres_dbname,
            user=postgres_user,
            password=postgres_password
        )
        cursor = conn.cursor()

        logger.info("Executing collect_schema_sizes function")

        # Execute the collect_schema_sizes function
        cursor.execute("SELECT ods_data_management.collect_schema_sizes();")
        conn.commit()

        logger.info("Schema sizes collected successfully")

    except Exception as e:
        logger.error(f"Error collecting schema sizes: {e}")
        raise

    finally:
        # Close the connection
        cursor.close()
        conn.close()
        logger.info("PostgreSQL connection closed")

with DAG(
    dag_id='collect_schema_sizes_dag',
    default_args=default_args,
    description='DAG to collect PostgreSQL schema sizes monthly',

   # schedule_interval='0 8 1 * *',  # At 08:00 on day-of-month 1, 2am pacific time
    schedule_interval='0 8 * * *',  # Every day at 08:00 system time, 2am  pacific time
    start_date=datetime(2023, 10, 1),
    catchup=False,
    tags=['schema-sizes'],
) as dag:

    start_task = EmptyOperator(
        task_id='start_task',
    )

    collect_schema_sizes_task = PythonOperator(
        task_id='collect_schema_sizes_task',
        python_callable=collect_schema_sizes,
    )

    start_task >> collect_schema_sizes_task
