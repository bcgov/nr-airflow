from airflow import DAG
from pendulum import datetime
from kubernetes import client
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

with DAG(
    start_date=datetime(2023, 11, 23),
    catchup=False,
    schedule=None,
    dag_id="dbt_example",
) as dag:
    run_ats_replication = KubernetesPodOperator(
        task_id="init_dbt_container",
        # image="ghcr.io/dbt-labs/dbt-postgres:1.7.2",
        # Abi: need to push image to TEST namespace or pull via GHCR
        image="image-registry.openshift-image-registry.svc:5000/a1b9b0-test/dbt-container-test@sha256:e166fc9e58837aaee1ed791914e844a206e75e610d856e53614022121b9c8cac",
        in_cluster=True,
        namespace="a1b9b0-test",
        service_account_name="airflow-admin",
        name="run_dbt_container",
        random_name_suffix=True,
        labels={"DataClass": "Low", "env": "dev"},
        reattach_on_restart=True,
        is_delete_operator_pod=False,
        get_logs=True,
        log_events_on_failure=True,
        container_resources= client.V1ResourceRequirements(
        requests={"cpu": "50m", "memory": "256Mi"},
        limits={"cpu": "1", "memory": "1Gi"}), 
        cmds=["dbt"], 
        arguments=["test"]
    )

