from airflow import DAG
from pendulum import datetime
from kubernetes import client
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret

vault_jwt = Secret("env", None, "nr-vault-jwt")

with DAG(
    start_date=datetime(2023, 11, 23),
    catchup=False,
    schedule=None,
    dag_id="vault_example",
) as dag:
    vault_action = KubernetesPodOperator(
        task_id="get_ods_host",
        image="ghcr.io/bcgov/nr-vault-patterns:main",
        in_cluster=True,
        namespace="a1b9b0-dev",
        name="get_ods_host",
        random_name_suffix=True,
        labels={"DataClass": "High", "Release": "test-release-af"},  # network policies
        reattach_on_restart=True,
        is_delete_operator_pod=True,
        get_logs=True,
        log_events_on_failure=True,
        secrets=[vault_jwt],
        container_resources= client.V1ResourceRequirements(
        requests={"cpu": "10m", "memory": "256Mi"},
        limits={"cpu": "50m", "memory": "500Mi"})
    )
