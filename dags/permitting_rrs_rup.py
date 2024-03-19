
from airflow import DAG
from pendulum import datetime
from kubernetes import client
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret

LOB = 'rrs-rup' # ats, fta, rrs, or lexis

ods_secrets = Secret("env", None, "ods-database")
lob_secrets = Secret("env", None, f"{LOB}-database")

with DAG(
    start_date=datetime(2023, 11, 23),
    catchup=False,
    schedule='0 6 20 * *',
    dag_id=f"permitting_pipeline_{LOB}",
) as dag:
    run_lexis_replication = KubernetesPodOperator(
        task_id="run_replication",
        image="ghcr.io/bcgov/nr-permitting-pipelines:main",
        image_pull_policy="Always",
        in_cluster=True,
        service_account_name="airflow-admin",
        name=f"run_{LOB}_replication",
        labels={"DataClass": "Medium", "ConnectionType": "database",  "Release": "airflow"},
        is_delete_operator_pod=False,
        secrets=[lob_secrets, ods_secrets],
        container_resources= client.V1ResourceRequirements(
        requests={"cpu": "50m", "memory": "512Mi"},
        limits={"cpu": "100m", "memory": "1024Mi"})
    )