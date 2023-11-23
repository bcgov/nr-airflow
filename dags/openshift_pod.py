from airflow import DAG
from pendulum import datetime
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from airflow.configuration import conf

namespace = conf.get("kubernetes", "NAMESPACE")

with DAG(
    start_date=datetime(2023, 11, 23),
    catchup=False,
    schedule="@daily",
    dag_id="openshift_pod",
) as dag:
    say_hello_name_in_haskell = KubernetesPodOperator(
        task_id="run_replication_script",
        image="image-registry.openshift-image-registry.svc:5000/a1b9b0-dev/permitting-pipeline-test@sha256:139156be349676036c1cba04898260592a11a947391d3bbc10e1a4f001339e04",
        in_cluster=True,
        namespace=namespace,
        name="oc_run_replication_script",
        # give the Pod name a random suffix, ensure uniqueness in the namespace
        random_name_suffix=True,
        labels={"DataClass": "Medium", "env": "dev"},
        # reattach to worker instead of creating a new Pod on worker failure
        reattach_on_restart=True,
        # delete Pod after the task is finished
        is_delete_operator_pod=True,
        get_logs=True,
        log_events_on_failure=True,
        env_vars={"NAMESPACE": f"{namespace}"},
    )