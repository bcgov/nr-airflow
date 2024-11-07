from airflow import DAG
from pendulum import datetime
from kubernetes import client
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from datetime import timedelta
import os

LOB = 'lrm'
sql_file_path = './Annual_Developed_Volume_Query.sql'
# For local development environment only.
ENV = os.getenv("AIRFLOW_ENV")

ods_secrets = Secret("env", None, f"{LOB}-ods-database")

if ENV == 'LOCAL':
    default_args = {
        'owner': 'BCTS',
        "email": ["sreejith.munthikodu@gov.bc.ca"],
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        "email_on_failure": False, # No alerts in local environment
        "email_on_retry": False,
    }
else:
    default_args = {
        'owner': 'BCTS',
        "email": ["sreejith.munthikodu@gov.bc.ca"],
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        "email_on_failure": True,
        "email_on_retry": False,
    }

with DAG(
    start_date=datetime(2024, 10, 23),
    catchup=False,
    schedule='0 5 * * MON-FRI',
    dag_id=f"bcts_transformations-{LOB}",
    default_args=default_args,
    description='DAG to run the transformations in ODS for BCTS Annual Developed Volume Dashboard',
) as dag:
    
    if ENV == 'LOCAL':

        run_replication = KubernetesPodOperator(
            task_id="run_transformation",
            image="nrids-bcts-data-pg-transformations:main",
            cmds=["python3", "./bcts_etl.py"],
            arguments=[sql_file_path],
            # Following configs are different in the local development environment
            # image_pull_policy="Always",
            # in_cluster=True,
            # service_account_name="airflow-admin",
            name=f"run_{LOB}_transformation_annual_developed_volume",
            labels={"DataClass": "Medium", "ConnectionType": "database",  "Release": "airflow"},
            is_delete_operator_pod=True,
            secrets=[ods_secrets],
            container_resources= client.V1ResourceRequirements(
            requests={"cpu": "50m", "memory": "512Mi"},
            limits={"cpu": "100m", "memory": "1024Mi"})
        )
    else:
        # In Dev, Test, and Prod Environments
        run_replication = KubernetesPodOperator(
            task_id="run_replication",
            image="ghcr.io/bcgov/nr-dap-ods-pg-transformations:main",
            cmds=["python3", "./bcts_etl.py"],
            arguments=[sql_file_path],
            image_pull_policy="Always",
            in_cluster=True,
            service_account_name="airflow-admin",
            name=f"run_{LOB}_transformation_annual_developed_volume",
            labels={"DataClass": "Medium", "ConnectionType": "database",  "Release": "airflow"},
            is_delete_operator_pod=True,
            secrets=[ods_secrets],
            container_resources= client.V1ResourceRequirements(
            requests={"cpu": "50m", "memory": "512Mi"},
            limits={"cpu": "100m", "memory": "1024Mi"}),
            random_name_suffix=False
        )