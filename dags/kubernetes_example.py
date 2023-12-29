from airflow import DAG
from pendulum import datetime
from kubernetes import client
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

# Work in progress

with DAG(
    start_date=datetime(2023, 12, 28),
    catchup=False,
    schedule=None,
    dag_id="kubernetes_example"
) as dag:
    run_ats_replication = KubernetesPodOperator(
        task_id="run_container",
        image="artifacts.developer.gov.bc.ca/docker-remote/python",
        image_pull_policy="IfNotPresent",
        image_pull_secrets="artifactory-pull",
        in_cluster=True,
        namespace="a1b9b0-test",
        service_account_name="airflow-admin",
        name="run_container",
        random_name_suffix=True,
        labels={"DataClass": "Low", "env": "dev"},
        reattach_on_restart=True,
        is_delete_operator_pod=False,
        get_logs=True,
        log_events_on_failure=True,
        container_resources= client.V1ResourceRequirements(
        requests={"cpu": "50m", "memory": "256Mi"},
        limits={"cpu": "1", "memory": "1Gi"}), 
        cmds=["python3"]
    )
