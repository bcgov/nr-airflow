from airflow import DAG
from pendulum import datetime
from kubernetes import client
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from datetime import timedelta
 
LOB = 'farm'
 
ods_secrets = Secret("env", None, f"{LOB}-ods-database")
lob_secrets = Secret("env", None, f"{LOB}-database")
 
default_args = {
    'owner': 'AGRIBRM',
    "email": ["NRM.DataFoundations@gov.bc.ca"],
    "email_on_failure": True,
    "email_on_retry": True,
}
 
with DAG(
    start_date=datetime(2024, 12, 12),
    catchup=False,
    schedule='0 14 * * *', ## 6am PST
    dag_id=f"agri-brm-pipeline-{LOB}",
    default_args=default_args,
    description='DAG to replicate FARM data to ODS',
) as dag:
    run_replication = KubernetesPodOperator(
        task_id="run_replication",
        image="ghcr.io/bcgov/nr-dap-ods-ora2pg:main",
        image_pull_policy="Always",
        in_cluster=True,
        service_account_name="airflow-admin",
        name=f"run_{LOB}_replication",
        labels={"DataClass": "Medium"},
        is_delete_operator_pod=False,
        secrets=[lob_secrets, ods_secrets],
        container_resources= client.V1ResourceRequirements(
        requests={"cpu": "50m", "memory": "512Mi"},
        limits={"cpu": "100m", "memory": "10Gi"})
    )
