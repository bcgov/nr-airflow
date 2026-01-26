from airflow import DAG
from datetime import datetime, timezone
from kubernetes import client
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import timedelta
import os

LOB = 'lrm'
# For local development environment only.
ENV = os.getenv("AIRFLOW_ENV")

ods_secrets = Secret("env", None, f"{LOB}-ods-database")
object_storage_secrets =  Secret("env", None, "bcts-s3-objectstorage") 
lob_secrets = Secret("env", None, f"{LOB}-database")


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
    dag_id=f"bcts_validate_write_off_forms",
    default_args=default_args,
    description='DAG to validate write-off forms for BCTS data in ODS',
) as dag:
  
    
    # In Dev, Test, and Prod Environments
    validate_write_off_forms = KubernetesPodOperator(
        task_id=f"bcts_validate_write_off_forms",
        image="ghcr.io/bcgov/nr-dap-ods-bctstransformations:BCTS-Update-write-off-forms-validation",
        cmds=["python3", "./bcts_validate_write_off_forms_transformation.py"],
        image_pull_policy="Always",
        in_cluster=True,
        service_account_name="airflow-admin",
        name=f"bcts-dap-write-off-forms-validation",
        labels={"DataClass": "Medium", "ConnectionType": "database",  "Release": "airflow"},
        is_delete_operator_pod=True,
        secrets=[ods_secrets, object_storage_secrets, lob_secrets],
        container_resources= client.V1ResourceRequirements(
        requests={"cpu": "50m", "memory": "512Mi"},
        limits={"cpu": "100m", "memory": "1024Mi"}),
        random_name_suffix=False
    )

