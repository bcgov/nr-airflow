from airflow import DAG
from pendulum import datetime
from kubernetes import client
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.operators.dummy import DummyOperator
from datetime import timedelta
import os

LOB = 'mof-client'
# For local development environment only.
ENV = os.getenv("AIRFLOW_ENV")

ods_secrets = Secret("env", None, f"lrm-ods-database")
lob_secrets = Secret("env", None, f"mof-client-database")


default_args = {
    'owner': 'BCTS',
    "email": ["NRM.DataFoundations@gov.bc.ca"],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry": False,
}

with DAG(
    start_date=datetime(2024, 10, 23),
    catchup=False,
    schedule='10 12 * * MON-FRI', # 4:10 AM PST
    dag_id=f"bcts-replication-client",
    default_args=default_args,
    description='DAG to replicate Timber Pricing CLIENT data to ODS for BCTS Performance Report',
) as dag:
        
    run_replication = KubernetesPodOperator(
        task_id="run_replication",
        image="ghcr.io/bcgov/nr-dap-ods-ora2pg:main",
        image_pull_policy="Always",
        in_cluster=True,
        service_account_name="airflow-admin",
        name=f"run_{LOB}_replication",
        labels={"DataClass": "Medium", "ConnectionType": "database",  "Release": "airflow"},
        is_delete_operator_pod=True,
        secrets=[lob_secrets, ods_secrets],
        container_resources= client.V1ResourceRequirements(
        requests={"cpu": "50m", "memory": "512Mi"},
        limits={"cpu": "100m", "memory": "1024Mi"}),
        random_name_suffix=False
    )

    task_completion_flag = DummyOperator(
        task_id='task_completion_flag'
    )

    run_replication >> task_completion_flag