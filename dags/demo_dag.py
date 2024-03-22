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
        image="artifacts.developer.gov.bc.ca/docker-remote/python",       # Container image to be used
        image_pull_policy="IfNotPresent",                                 # Policy for pulling the Docker image
        image_pull_secrets="artifactory-pull",                            # Image pull secret (only required when using Artifactory)
        service_account_name="airflow-admin",                             # Service account to be used for running the pod
        name="run_container",                                             # Name of the pod                                     # Add a random suffix to the pod name
        labels={"DataClass": "Low", "env": "dev"},                        # Labels to be assigned to the pod
        log_events_on_failure=True,                                       # Log events in case of task failure
        container_resources=client.V1ResourceRequirements(
            requests={"cpu": "50m", "memory": "256Mi"},                   # Resource requests for the container
            limits={"cpu": "1", "memory": "1Gi"}),                         # Resource limits for the container
        cmds=["python3"])                                                 # Command to be executed in the container

    )
