from airflow import DAG
from pendulum import datetime
from kubernetes import client
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret

ods_secrets = Secret("env", None, "ods-database")
lh_secrets = Secret("env", None, "foriegn-data-wrapper-ods-dlh-dev")

with DAG(
    start_date=datetime(2024, 02, 07),
    catchup=False,
    schedule=None,
    dag_id="dbt_dlh_pmt_example",
) as dag:
    run_ats_replication = KubernetesPodOperator(
        task_id="init_dbt_container",
        secrets=[ods_secrets, lh_secrets],      
        image="ghcr.io/bcgov/nr-dap-dlh-pmt:main",
        image_pull_policy="Always",
        in_cluster=True,
        namespace="a1b9b0-dev",
        service_account_name="airflow-admin",
        name="run_dbt_container",
        random_name_suffix=True,
        labels={"DataClass": "Low", "env": "dev", "ConnectionType": "database"},
        reattach_on_restart=True,
        is_delete_operator_pod=False,
        get_logs=True,
        log_events_on_failure=True,
        container_resources= client.V1ResourceRequirements(
        requests={"cpu": "50m", "memory": "256Mi"},
        limits={"cpu": "1", "memory": "1Gi"}),
        cmds=["dbt"], 
        arguments=["snapshot","--select","dim_org","--profiles-dir","/usr/app/dbt/.dbt"]
        # arguments=["test","--profiles-dir","/usr/app/dbt/.dbt"]
    )

