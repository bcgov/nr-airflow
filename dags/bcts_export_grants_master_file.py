from airflow import DAG
from pendulum import datetime
from kubernetes import client
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from datetime import timedelta
import os

LOB = 'lrm'
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
    schedule=None,
    dag_id=f"bcts_export-grants",
    default_args=default_args,
    description='DAG to export the grants master file to ODS',
) as dag:
    
    if ENV == 'LOCAL':

        run_replication = KubernetesPodOperator(
            task_id=f"export_bcts_grants",
            image="nrids-bcts-data-pg-access:main",
            cmds=["python3", "./bcts_access_export_master_file.py"],
            # Following configs are different in the local development environment
            # image_pull_policy="Always",
            # in_cluster=True,
            # service_account_name="airflow-admin",
            name=f"export_{LOB}_access_grants",
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
            task_id=f"export_bcts_grants",
            image="ghcr.io/bcgov/nr-dap-ods-bctsgrantmngmt:SD-128488-BCTS-ODS-GRANT-MANAGEMENT",
            cmds=["python3", "./bcts_access_export_master_file.py"],
            image_pull_policy="Always",
            in_cluster=True,
            service_account_name="airflow-admin",
            name=f"export_{LOB}_access_grants",
            labels={"DataClass": "Medium", "ConnectionType": "database",  "Release": "airflow"},
            is_delete_operator_pod=True,
            secrets=[ods_secrets],
            container_resources= client.V1ResourceRequirements(
            requests={"cpu": "50m", "memory": "512Mi"},
            limits={"cpu": "100m", "memory": "1024Mi"}),
            random_name_suffix=False
        )