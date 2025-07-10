from airflow import DAG
from pendulum import datetime
from kubernetes import client
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from datetime import timedelta

rar_secrets = Secret("env", None, "rar-data-extract")

default_args = {
    'owner': 'Data Foundations',
    "email": ["NRM.DataFoundations@gov.bc.ca"],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry": True,
}

with DAG(
    start_date=datetime(2024, 7, 23),
    catchup=False,
    schedule='0 4 * * *',
    dag_id="housing-pipeline-rar",
    default_args=default_args,
    description='DAG to replicate RAR data extract to S3 bucket so that it can be accessed via BCBox',
) as dag:
    run_replication = KubernetesPodOperator(
        task_id="run_replication",
        image="ghcr.io/bcgov/nr-dap-ods-ora2s3:main",
        image_pull_policy="Always",
        in_cluster=True,
        service_account_name="airflow-admin",
        name=f"run_rar_s3_replication",
        labels={"DataClass": "Medium",
                "ConnectionType": "database",
                "Release": "airflow"},
        is_delete_operator_pod=True,
        secrets=[rar_secrets],
        container_resources= client.V1ResourceRequirements(
        requests={"cpu": "50m", "memory": "512Mi"},
        limits={"cpu": "100m", "memory": "1024Mi"})
    )
