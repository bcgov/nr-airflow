from airflow import DAG
from pendulum import datetime
from kubernetes import client
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret

ods_secrets = Secret("env", None, "ods-database")
ats_housing_secrets = Secret("env", None, "ats-database-housing")

default_args = {
    "email": ["NRM.DataFoundations@gov.bc.ca"],
    "email_on_failure": True,
    "email_on_retry": True,
}

with DAG(
    start_date=datetime(2024, 2, 20),
    catchup=False,
    schedule=None,
    dag_id="lob_dq_ats_housing",
    default_args=default_args,
) as dag:
    run_ats_replication = KubernetesPodOperator(
        task_id="run_ats_housing_replication",
        image="ghcr.io/bcgov/nr-dap-ods:main",
        image_pull_policy="Always",
        # image="image-registry.openshift-image-registry.svc:5000/a1b9b0-dev/data-replication-parametrized-audit1@sha256:8c51ee820434e4f5d06a91deda645bcd0a943b8c87bc3c8a8e67dead1c18a786",
        in_cluster=True,
        service_account_name="airflow-admin",
        name="run_ats_housing_replication",
        random_name_suffix=True,
        labels={"DataClass": "Medium", "ConnectionType": "database", "Release": "test-release-af"},  # network policies
        reattach_on_restart=True,
        is_delete_operator_pod=False,
        get_logs=True,
        log_events_on_failure=True,
        secrets=[ats_housing_secrets, ods_secrets],
        container_resources= client.V1ResourceRequirements(
        requests={"cpu": "10m", "memory": "256Mi"},
        limits={"cpu": "50m", "memory": "500Mi"})
    )
