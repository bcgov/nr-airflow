from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import os
from github import Github
import re

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def execute_python_script():
    
    # Retrieve PostgreSQL connection parameters from Airflow variables
    postgres_host = Variable.get('ODS_HOST')
    postgres_port = Variable.get('ODS_PORT')
    postgres_dbname = Variable.get('ODS_DATABASE')
    postgres_user = Variable.get('ODS_USERNAME')
    postgres_password = Variable.get('ODS_PASSWORD')
    github_secret = Variable.get('GITHUB_SECRET')

    # Function to connect to PostgreSQL and scan for new applications
    def scan_table_for_new_apps():
        conn = psycopg2.connect(
            host=postgres_host,
            port=postgres_port,
            dbname=postgres_dbname,
            user=postgres_user,
            password=postgres_password
        )
        cursor = conn.cursor()

        cursor.execute("SELECT DISTINCT application_name FROM ods_data_management.cdc_master_table_list")
        applications = cursor.fetchall()

        cursor.close()
        conn.close()

        return applications

    # Main function to orchestrate the process
    def main():
        new_applications = scan_table_for_new_apps()
        print(new_applications)

    if __name__ == "__main__":
        main()
    print(new_applications = scan_table_for_new_apps())

with DAG(
    dag_id='auto_dag_creation',
    default_args=default_args,
    description='Execute Python script',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 5, 14),
    tags=['example'],
) as dag:

    execute_script_task = PythonOperator(
        task_id='execute_script',
        python_callable=execute_python_script
    )
