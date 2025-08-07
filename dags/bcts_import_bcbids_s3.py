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
s3_secrets = Secret("env", None, f"bcts-s3-objectstorage")


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
    schedule='35 12 * * MON-FRI',
    dag_id=f"bcts_import_bcbids_data",
    default_args=default_args,
    description='DAG to copy webcrawled BCBIDS data in object storage to ODS.',
) as dag:
    
    # In Dev, Test, and Prod Environments
    bcts_import_bcbids_data = KubernetesPodOperator(
        task_id="bcts_import_bcbids_data",
        image="ghcr.io/bcgov/nr-dap-ods-s3_2pg_bcts:DAPBCTS-5-WEEKLY-TSL-REPORT",
        cmds=["python3", "./s3_to_pg.py"],
        image_pull_policy="Always",
        in_cluster=True,
        service_account_name="airflow-admin",
        name=f"run_bcts_import_s3_bcbids_data",
        labels={"DataClass": "Medium", "ConnectionType": "database",  "Release": "airflow"},
        is_delete_operator_pod=True,
        secrets=[ods_secrets, s3_secrets],
        container_resources= client.V1ResourceRequirements(
        requests={"cpu": "50m", "memory": "512Mi"},
        limits={"cpu": "100m", "memory": "1024Mi"}),
        random_name_suffix=False
    )


    task_completion_flag = DummyOperator(
        task_id='task_completion_flag'
    )

    bcts_import_bcbids_data >> task_completion_flag 
    