from airflow import DAG
from pendulum import datetime
from kubernetes import client
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.operators.dummy import DummyOperator
from datetime import timedelta
import os

LOB = 'lrm'
# For local development environment only.
ENV = os.getenv("AIRFLOW_ENV")

ods_secrets = Secret("env", None, f"{LOB}-ods-database")
lob_secrets = Secret("env", None, f"{LOB}-database")

if ENV == 'LOCAL':
    default_args = {
        'owner': 'BCTS',
        "email": ["NRM.DataFoundations@gov.bc.ca"],
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        "email_on_failure": False, # No alerts in local environment
        "email_on_retry": False,
    }
else:
    default_args = {
        'owner': 'BCTS',
        "email": ["NRM.DataFoundations@gov.bc.ca"],
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        "email_on_failure": True,
        "email_on_retry": False,
    }

with DAG(
    start_date=datetime(2024, 10, 23),
    catchup=False,
    schedule='30 12 * * MON-FRI', # 4 AM PST
    dag_id=f"bcts-replication-lrm",
    default_args=default_args,
    description='DAG to replicate LRM data to ODS for BCTS Annual Developed Volume Dashboard',
) as dag:
    
    if ENV == 'LOCAL':

        run_replication = KubernetesPodOperator(
            task_id="run_replication",
            image="nrids-bcts-data-ora2pg:main",
            # Following configs are different in the local development environment
            # image_pull_policy="Always",
            # in_cluster=True,
            # service_account_name="airflow-admin",
            name=f"run_{LOB}_replication",
            labels={"DataClass": "Medium", "ConnectionType": "database",  "Release": "airflow"},
            is_delete_operator_pod=True,
            secrets=[lob_secrets, ods_secrets],
            container_resources= client.V1ResourceRequirements(
            requests={"cpu": "50m", "memory": "512Mi"},
            limits={"cpu": "100m", "memory": "1024Mi"})
        )
        
    else:
        # In Dev, Test, and Prod Environments
        run_replication = KubernetesPodOperator(
            task_id="run_replication",
            image="ghcr.io/bcgov/nr-dap-ods-ora2pg:SD-140653-REPLICATE-LRM-FORESTVIEW-VIEWS",
            image_pull_policy="Always",
            in_cluster=True,
            service_account_name="airflow-admin",
            name=f"run_{LOB}_replication",
            labels={"DataClass": "Medium", "ConnectionType": "database",  "Release": "airflow"},
            is_delete_operator_pod=True,
            secrets=[lob_secrets, ods_secrets],
            container_resources= client.V1ResourceRequirements(
            requests = {"cpu": "400m", "memory": "6144Mi"},
            limits = {"cpu": "800m", "memory": "8192Mi"}),
            random_name_suffix=False
        )

    task_completion_flag = DummyOperator(
        task_id='task_completion_flag'
    )

    run_replication >> task_completion_flag