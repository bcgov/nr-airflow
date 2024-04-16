from airflow import DAG
from pendulum import datetime
from kubernetes import client
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret

LOB = 'lexis'

ods_secrets = Secret("env", None, "ods-database")
lexis_secrets = Secret("env", None, f"{LOB}-database") # Replace "LOB" with source system name (e.g. 'lexis')

default_args = {
    "email": ["NRM.DataFoundations@gov.bc.ca"],
    "email_on_failure": True,
    "email_on_retry": True,
}

with DAG(
    start_date=datetime(2023, 11, 23),
    catchup=False,
    schedule=None,
    dag_id=f"permitting_pipeline_{LOB}", # Replace "LOB" with source system name (e.g. 'lexis')
    default_args=default_args,
) as dag:
    run_lexis_replication = KubernetesPodOperator( # Replace "LOB" with source system name (e.g. 'lexis')
        task_id=f"run_{LOB}_replication", # Replace "LOB" with source system name (e.g. 'lexis')
        image="ghcr.io/bcgov/nr-permitting-pipelines:main",
        image_pull_policy="Always",
        in_cluster=True,
        namespace="a1b9b0-dev",
        service_account_name="airflow-admin",
        name=f"run_{LOB}_replication", # Replace "LOB" with source system name (e.g. 'lexis')
        random_name_suffix=True,
        labels={"DataClass": "Medium", "ConnectionType": "database",  "Release": "test-release-af"}, 
        reattach_on_restart=True,
        is_delete_operator_pod=False,
        get_logs=True,
        log_events_on_failure=True,
        secrets=[lexis_secrets, ods_secrets], # Replace "LOB" with source system name (e.g. 'lexis')
        container_resources= client.V1ResourceRequirements(
        requests={"cpu": "50m", "memory": "512Mi"},
        limits={"cpu": "100m", "memory": "1024Mi"})
    )
