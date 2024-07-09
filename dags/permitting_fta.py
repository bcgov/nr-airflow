from airflow import DAG
from pendulum import datetime
from kubernetes import client
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from datetime import timedelta

LOB = 'fta'

ods_secrets = Secret("env", None, f"{LOB}-ods-database")
lob_secrets = Secret("env", None, f"{LOB}-database")

default_args = {
    'owner': 'PMT',
    "email": ["NRM.DataFoundations@gov.bc.ca"],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry": True,
}

with DAG(
    start_date=datetime(2023, 11, 23),
    catchup=False,
    schedule='5 12 * * *',
    dag_id=f"permitting-pipeline-{LOB}",
    default_args=default_args,
    description='DAG to replicate FTA data to ODS for X-NRS Permitting Dashboard'
) as dag:
    run_replication = KubernetesPodOperator(
        task_id="run_replication",
        image="ghcr.io/bcgov/nr-dap-ods:main",
        image_pull_policy="Always",
        in_cluster=True,
        service_account_name="airflow-admin",
        name=f"run_{LOB}_replication",
        labels={"DataClass": "Medium", "ConnectionType": "database",  "Release": "airflow"},
        is_delete_operator_pod=True,
        secrets=[lob_secrets, ods_secrets],
        container_resources= client.V1ResourceRequirements(
        requests={"cpu": "50m", "memory": "512Mi"},
        limits={"cpu": "100m", "memory": "1024Mi"})
    )