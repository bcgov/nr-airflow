from airflow import DAG
from pendulum import datetime
from kubernetes import client
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret

ods_secrets = Secret("env", None, "ods-database")
ats_connectivity_secrets = Secret("env", None, "ats-database-connectivity")

default_args = {
    'owner': 'Data Foundations',
    "email": ["NRM.DataFoundations@gov.bc.ca"],
    "email_on_failure": True,
    "email_on_retry": True,
}

with DAG(
    start_date=datetime(2024, 2, 20),
    catchup=False,
    schedule=None,
    dag_id="connectivity-query-ats",
    default_args=default_args,
    description='DAG to create table of monthly ATS connectivity licenses'
) as dag:
    run_ats_replication = KubernetesPodOperator(
        task_id="run_ats_connectivity_replication",
        image="ghcr.io/bcgov/nr-dap-ods-ora2pg:main",
        image_pull_policy="Always",
        in_cluster=True,
        service_account_name="airflow-admin",
        name="run_ats_connectivity_replication",
        random_name_suffix=True,
        labels={"DataClass": "Medium", "ConnectionType": "database", "Release": "airflow"},  # network policies
        reattach_on_restart=True,
        is_delete_operator_pod=False,
        get_logs=True,
        log_events_on_failure=True,
        secrets=[ats_connectivity_secrets, ods_secrets],
        container_resources= client.V1ResourceRequirements(
        requests={"cpu": "10m", "memory": "256Mi"},
        limits={"cpu": "50m", "memory": "500Mi"})
    )
