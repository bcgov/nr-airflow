from airflow import DAG
from pendulum import datetime
from kubernetes import client
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret

with DAG(
    start_date=datetime(2023, 11, 23),
    catchup=False,
    schedule=None,
    dag_id="dbt_rrs_snapshot",
) as dag:
    run_rrs_snapshot = KubernetesPodOperator(
        task_id="run_rrs_snapshot",
        image="image-registry.openshift-image-registry.svc:5000/a1b9b0-dev/dbt-container-snapshot@sha256:c1a7a01e2908cc1b12ec47923cbe12ed4b46a99b2ac15ff43955e53e152d8498",
        in_cluster=True,
        namespace="a1b9b0-dev",
        service_account_name="airflow-admin",
        name="run_rrs_snapshot",
        random_name_suffix=True,
        labels={"DataClass": "Medium", "ConnectionType": "database"},  # network policies
        reattach_on_restart=True,
        is_delete_operator_pod=False,
        get_logs=True,
        log_events_on_failure=True,
        container_resources= client.V1ResourceRequirements(
        requests={"cpu": "10m", "memory": "256Mi"},
        limits={"cpu": "50m", "memory": "500Mi"})
    )