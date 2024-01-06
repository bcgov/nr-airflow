from airflow import DAG
from pendulum import datetime
from kubernetes import client
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret

with DAG(
    start_date=datetime(2023, 11, 23),
    catchup=False,
    schedule=None,
    dag_id="dbt_dlh_example",
) as dag:
    run_ats_replication = KubernetesPodOperator(
        task_id="init_dbt_container",
        image="image-registry.apps.emerald.devops.gov.bc.ca/a1b9b0-dev/dbt-project-dlh:v1",
        # Abi: the GHCR container below is a WIP - need to find a way to inject secrets into the profiles.yml
        # image="ghcr.io/bcgov/nr-dbt-project:oc-adjustments",
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
        arguments=["run","--select","road_tenure_type_code_agg","--profiles-dir","/usr/app/dbt/.dbt"]
        # arguments=["snapshot", "--profiles-dir", "/usr/app/dbt/.dbt"]
        # Next step: configmap for profile.yml
    )

