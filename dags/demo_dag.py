from airflow import DAG
from pendulum import datetime
from kubernetes import client
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

with DAG(
    start_date=datetime(2024, 3, 20),
    catchup=False,
    schedule=None,
    dag_id="demo-dag"
) as dag:
    python_container = KubernetesPodOperator(
        task_id="run_container",
        image="artifacts.developer.gov.bc.ca/docker-remote/python",
        image_pull_policy="IfNotPresent",
        image_pull_secrets="artifactory-pull",
        in_cluster=True,
        service_account_name="airflow-admin",
        name="run_container",
        labels={"DataClass": "Low", "env": "prod"},
        container_resources= client.V1ResourceRequirements(
        requests={"cpu": "50m", "memory": "68Mi"},
        limits={"cpu": "100m", "memory": "200Mi"}),
        cmds=["python3"]
    )
