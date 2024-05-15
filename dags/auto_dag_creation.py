from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
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
    'retry_delay': timedelta(minutes=1),
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
    
    g = Github(github_secret)
    repo = g.get_repo("bcgov/nr-airflow")
    contents = repo.get_contents("/dags", ref="a1b9b0-dev")
    existing_dags= [c.path for c in contents]
    existing_dag_list = []
    prefix = "dags/pipeline-"
    suffix = ".py"
    for item in existing_dags:
        # Remove the prefix and suffix
        name = item[len(prefix):-len(suffix)]
        existing_dag_list.append(name)
    
    for application in applications:
        application = application[0]  # Extract application name
        if application not in existing_dag_list:
                dag_id = f"pipeline-{application}"
                app = application
                #LOB = application
                DAG = f""" 
from airflow import DAG
from pendulum import datetime
from kubernetes import client
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from datetime import timedelta

LOB = "{app}"
LOB = LOB.lower()

default_args = {{
    "email": ["NRM.DataFoundations@gov.bc.ca"],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry": True,
}}

with DAG(
    start_date=datetime(2023, 11, 23),
    catchup=False,
    schedule='5 12 * * *',
    dag_id=f"pipeline-{{LOB}}",
    default_args=default_args,
) as dag:
    ods_secrets = Secret("env", None, "ods-database")
    lob_secrets = Secret("env", None, f"{{LOB}}-database")

    run_replication = KubernetesPodOperator(
        task_id="run_replication",
        image="ghcr.io/bcgov/nr-dap-ods:main",
        image_pull_policy="Always",
        in_cluster=True,
        service_account_name="airflow-admin",
        name=f"run_{{LOB}}_replication",
        labels={{"DataClass": "Medium", "ConnectionType": "database",  "Release": "airflow"}},
        is_delete_operator_pod=False,
        secrets=[lob_secrets, ods_secrets],
        container_resources=client.V1ResourceRequirements(
            requests={{"cpu": "50m", "memory": "512Mi"}},
            limits={{"cpu": "100m", "memory": "1024Mi"}}
        )
    )"""
                DAG = str(DAG)

                g = Github(github_secret)
                repo = g.get_repo('bcgov/nr-airflow')

                repo.create_file(f'dags/{dag_id}.py', 'upload dags', DAG, branch='a1b9b0-dev')
                existing_dag_list.append(application)


with DAG(
    dag_id='auto_dag_creation',
    default_args=default_args,
    description='Execute Python script',
    schedule_interval='0 * * * *',
    start_date=datetime(2024, 5, 14),
    tags=['ui-automation'],
) as dag:

    start_task = EmptyOperator(
        task_id='start_task', # The name of the sub-task in the workflow.
        dag=dag # When using the "with Dag(...)" syntax you could leave this out
    )

    execute_script_task = PythonOperator(
        task_id='execute_script',
        python_callable=execute_python_script
    )
    
    start_task >> execute_script_task
