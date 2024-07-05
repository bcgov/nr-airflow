from airflow import DAG
from pendulum import datetime
from kubernetes import client
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.operators.bash import BashOperator

vault_jwt = Secret("env", None, "nr-vault-jwt")

with DAG(
    start_date=datetime(2023, 11, 23),
    catchup=False,
    schedule=None,
    dag_id="vault_example",
) as dag:
    vault_action = KubernetesPodOperator(
        task_id="get_ods_host",
        image="ghcr.io/bcgov/nr-vault-patterns:main",
        image_pull_policy="Always",
        in_cluster=True,
        name="get_ods_host",
        random_name_suffix=True,
        labels={"DataClass": "High", "Release": "airflow"},  # network policies
        reattach_on_restart=True,
        is_delete_operator_pod=False,
        get_logs=True,
        log_events_on_failure=True,
        secrets=[vault_jwt],
        env_vars={"VAULT_ENV": "dev", "SECRET_NAME": "admin-ods-dev"},
        do_xcom_push=True, # allows pushing the secrets to return.json
        container_resources= client.V1ResourceRequirements(
        requests={"cpu": "10m", "memory": "256Mi"},
        limits={"cpu": "50m", "memory": "500Mi"})
    )

#    xcom_value = task_instance.xcom_pull(task_ids='get_ods_host')

#    print(xcom_value)
    
pod_task_xcom_result = BashOperator(
    bash_command="echo \"{{ task_instance.xcom_pull('get_ods_host') }}\"",
    task_id="pod_task_xcom_result",
)

vault_action >> pod_task_xcom_result
