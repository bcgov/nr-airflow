from airflow import DAG
from pendulum import datetime
from kubernetes import client
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.models import Variable

ods_password = Variable.get("ods_password")
dlh_password = Variable.get("dlh_password")

with DAG(
    start_date=datetime(2024, 2, 13),
    catchup=False,
    schedule=None,
    dag_id="demo2_dbt_summarize_org_unit",
) as dag:
   dbt_summarize_org_unit = KubernetesPodOperator(
        task_id="init_dbt_container",      
        image="ghcr.io/bcgov/nr-dap-dlh-pmt:main",
        image_pull_policy="Always",
        in_cluster=True,
        namespace="a1b9b0-dev",
        service_account_name="airflow-admin",
        name="run_dbt_container",
        random_name_suffix=True,
        labels={"DataClass": "Low", "env": "dev", "ConnectionType": "database"},
        env_vars={"ods_password": ods_password, "dlh_password": dlh_password},
        reattach_on_restart=True,
        is_delete_operator_pod=False,
        get_logs=True,
        log_events_on_failure=True,
        container_resources= client.V1ResourceRequirements(
        requests={"cpu": "50m", "memory": "256Mi"},
        limits={"cpu": "1", "memory": "1Gi"}),
        cmds=["dbt"], 
        arguments=["snapshot","--select","dim_org","--profiles-dir","/usr/app/dbt/.dbt"]

    )

