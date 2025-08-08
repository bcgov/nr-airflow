from airflow import DAG
from datetime import datetime, timezone
from kubernetes import client
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.dummy import DummyOperator
from datetime import timedelta
import os

LOB = 'lrm'
# For local development environment only.
ENV = os.getenv("AIRFLOW_ENV")

ods_secrets = Secret("env", None, f"{LOB}-ods-database")


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
    schedule='35 12 * * *',
    dag_id=f"bcts_audit_replication",
    default_args=default_args,
    description='DAG to copy FTA tables to BCTS staging area.',
) as dag:
    
    # In Dev, Test, and Prod Environments
    bcts_audit_replication = KubernetesPodOperator(
        task_id="bcts_audit_replication",
        image="ghcr.io/bcgov/nr-dap-ods-bctstransformations:BCTS-ETL-FIXES",
        cmds=["python3", "./bcts_audit_replication.py"],
        image_pull_policy="Always",
        in_cluster=True,
        service_account_name="airflow-admin",
        name=f"bcts_audit_replication",
        labels={"DataClass": "Medium", "ConnectionType": "database",  "Release": "airflow"},
        is_delete_operator_pod=True,
        secrets=[ods_secrets],
        container_resources= client.V1ResourceRequirements(
        requests={"cpu": "50m", "memory": "512Mi"},
        limits={"cpu": "100m", "memory": "1024Mi"}),
        random_name_suffix=False
    )


    task_completion_flag = DummyOperator(
        task_id='task_completion_flag'
    )

    bcts_import_fta_data >> task_completion_flag 
    