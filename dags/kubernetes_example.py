from airflow import DAG
from pendulum import datetime
from kubernetes import client
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret

ods_secrets = Secret("env", None, "ods-database")

with DAG(
    start_date=datetime(2023, 11, 23),
    catchup=False,
    schedule=None,
    dag_id="kubernetes_example",
) as dag:
    run_ats_replication = KubernetesPodOperator(
        task_id="run_container",
        image="image-registry.openshift-image-registry.svc:5000/a1b9b0-dev/python@sha256:b94af75d4ff65c50bf1b2119bca6d0ba707037bacd0cb75314801a6953c03241",
        in_cluster=True,
        namespace="a1b9b0-dev",
        service_account_name="airflow-admin",
        name="run_container",
        random_name_suffix=True,
        labels={"DataClass": "Low", "env": "dev"},
        reattach_on_restart=True,
        is_delete_operator_pod=False,
        get_logs=True,
        log_events_on_failure=True,
        secrets=[ods_secrets],
        container_resources= client.V1ResourceRequirements(
        requests={"cpu": "50m", "memory": "256Mi"},
        limits={"cpu": "1", "memory": "1Gi"}), 
        cmds=["python3", "print(ods_secrets)"]
    )
