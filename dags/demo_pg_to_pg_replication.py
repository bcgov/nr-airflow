from airflow import DAG
from pendulum import datetime
from kubernetes import client
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret

ods_secrets = Secret("env", None, "ods-database-src")
dlh_secrets = Secret("env", None, "dlh-database-tgt")

default_args = {
    "email": ["NRM.DataFoundations@gov.bc.ca"],
    "email_on_failure": True,
    "email_on_retry": True,
}

with DAG(
    start_date=datetime(2023, 11, 23),
    catchup=False,
    schedule=None,
    dag_id="demo_pg_to_pg_replication",
    default_args=default_args,
) as dag:
    run_ats_replication = KubernetesPodOperator(
        task_id="run_pg_to_pg_replication",
        image="ghcr.io/bcgov/nr-dap-ods-pg2pg:ods-replication-pg2pg",
        image_pull_policy="Always",
        # image="image-registry.openshift-image-registry.svc:5000/a1b9b0-dev/data-replication-parametrized-audit1@sha256:8c51ee820434e4f5d06a91deda645bcd0a943b8c87bc3c8a8e67dead1c18a786",
        in_cluster=True,
        namespace="a1b9b0-dev",
        service_account_name="airflow-admin",
        name="run_pg_to_pg_replication",
        random_name_suffix=True,
        labels={"DataClass": "Medium", "ConnectionType": "database", "Release": "test-release-af"},  # network policies
        reattach_on_restart=True,
        is_delete_operator_pod=False,
        get_logs=True,
        log_events_on_failure=True,
        secrets=[ods_secrets, dlh_secrets],
        container_resources= client.V1ResourceRequirements(
        requests={"cpu": "10m", "memory": "256Mi"},
        limits={"cpu": "50m", "memory": "500Mi"})
    )
