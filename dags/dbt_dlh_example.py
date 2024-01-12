from airflow import DAG
from pendulum import datetime
from kubernetes import client
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret

ods_secrets = Secret("env", None, "ods-database")
lh_secrets = Secret("env", None, "lh-database")

with DAG(
    start_date=datetime(2023, 11, 23),
    catchup=False,
    schedule=None,
    dag_id="dbt_dlh_example",
) as dag:
    run_ats_replication = KubernetesPodOperator(
        task_id="init_dbt_container",
        secrets=[ods_secrets, lh_secrets],      
        # Abi: the GHCR container below is a WIP - need to set up containers for each folder
        # image="image-registry.openshift-image-registry.svc:5000/a1b9b0-dev/dbt-project-ods-dlh@sha256:2a36918cb6ac8ffe233c63a714709a78c587b95bfca6c47cd9539344be8be372",
        image="ghcr.io/bcgov/nr-dbt-project:main",
        image_pull_policy="Always"
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
        #arguments=["run","--select","road_tenure_type_code_agg","--profiles-dir","/usr/app/dbt/.dbt"]
        arguments=["test","--profiles-dir","/usr/app/dbt/.dbt"]
    )

