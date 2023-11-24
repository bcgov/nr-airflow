from airflow import DAG
from pendulum import datetime
from kubernetes import client
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret

ods_secrets = Secret("env", None, "ods-database")
ats_secrets = Secret("env", None, "ats-database")

with DAG(
    start_date=datetime(2023, 11, 23),
    catchup=False,
    schedule=None,
    dag_id="permitting_pipeline",
) as dag:
    run_ats_replication = KubernetesPodOperator(
        task_id="run_ats_replication",
        image="image-registry.openshift-image-registry.svc:5000/a1b9b0-dev/data-replication-parametrized@sha256:8562520bc8ea4ca68f3abc0b45736c3fa0d59b3613aa31da95f60c29214d5abe",
        in_cluster=True,
        namespace="a1b9b0-dev",
        service_account_name="airflow-admin",
        name="run_ats_replication",
        random_name_suffix=True,
        labels={"DataClass": "Medium", "env": "dev"},
        reattach_on_restart=True,
        is_delete_operator_pod=False,
        get_logs=True,
        log_events_on_failure=True,
        secrets=[ats_secrets, ods_secrets],
        container_resources= client.V1ResourceRequirements(
        requests={"cpu": "50m", "memory": "256Mi"},
        limits={"cpu": "1", "memory": "1Gi"})
    )