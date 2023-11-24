from airflow import DAG
from pendulum import datetime
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

with DAG(
    start_date=datetime(2023, 11, 23),
    catchup=False,
    schedule=None,
    dag_id="permitting_pipeline",
) as dag:
    oc_run_replication_script = KubernetesPodOperator(
        task_id="run_replication_script",
        image="image-registry.openshift-image-registry.svc:5000/a1b9b0-dev/data-replication-parametrized@sha256:8562520bc8ea4ca68f3abc0b45736c3fa0d59b3613aa31da95f60c29214d5abe",
        in_cluster=True,
        namespace="a1b9b0-dev",
        name="oc_run_replication_script",
        random_name_suffix=True,
        labels={"DataClass": "Medium", "env": "dev"},
        reattach_on_restart=True,
        is_delete_operator_pod=False,
        get_logs=True,
        log_events_on_failure=True,
        # env_vars={"namespace": f"{namespace}"}
        # container_resources=None,
        # service_account_name="airflow-admin",
        # configmaps=["fta-pipeline"]
        secrets=["ods-database", "fta-database"]
    )