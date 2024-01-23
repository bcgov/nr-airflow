from airflow import DAG
from pendulum import datetime
from kubernetes import client
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

with DAG(
    start_date=datetime(2023, 12, 28),
    catchup=False,
    schedule='0 6 * * *',  # schedule 10pm PST
    dag_id="emli_demo" 
) as dag:
    python_container = KubernetesPodOperator(
        task_id="emli_container",
        image="artifacts.developer.gov.bc.ca/docker-remote/python", # any containerized language, tool
        image_pull_policy="IfNotPresent",
        image_pull_secrets="artifactory-pull",
        in_cluster=True,
        namespace="a1b9b0-dev",
        service_account_name="airflow-admin",
        name="emli_container",
        random_name_suffix=True,
        labels={"DataClass": "Low", "env": "dev"}, # data classification
        reattach_on_restart=True,
        is_delete_operator_pod=False,
        get_logs=True,
        log_events_on_failure=True,
        container_resources= client.V1ResourceRequirements(
        requests={"cpu": "50m", "memory": "256Mi"}, # resource allocation
        limits={"cpu": "1", "memory": "1Gi"}), 
        cmds=["python3"] # commands to run
    )
