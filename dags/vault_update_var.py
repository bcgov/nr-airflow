from airflow import DAG, settings
from pendulum import datetime
from kubernetes import client
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

vault_jwt = Secret("env", None, "nr-vault-jwt")

with DAG(
    start_date=datetime(2024, 2, 12),
    catchup=False,
    schedule=None,
    dag_id="vault_update_var",
) as dag:
    vault_action = KubernetesPodOperator(
        task_id="get_ods_host",
        image="ghcr.io/bcgov/nr-vault-patterns:main",
        image_pull_policy="Always",
        in_cluster=True,
        namespace="a1b9b0-dev",
        name="get_ods_host",
        random_name_suffix=True,
        labels={"DataClass": "High", "Release": "test-release-af"},  # network policies
        reattach_on_restart=True,
        is_delete_operator_pod=False,
        get_logs=True,
        log_events_on_failure=True,
        secrets=[vault_jwt],
        env_vars={"VAULT_ENV": "dev", "SECRET_NAME": "ods-dev"}, 
        do_xcom_push=True, # allows pushing the secrets to return.json
        container_resources= client.V1ResourceRequirements(
        requests={"cpu": "10m", "memory": "256Mi"},
        limits={"cpu": "50m", "memory": "500Mi"})
    )

    pod_task_xcom_result = BashOperator(
        bash_command="echo \"{{ task_instance.xcom_pull('get_ods_host') }}\"",
        task_id="pod_task_xcom_result",
    )

    vault_action >> pod_task_xcom_result

def update_ods_host_variables(task_instance, **kwargs):
    result = task_instance.xcom_pull(task_ids='get_ods_host')
    # Assuming result is a dictionary containing keys: 'ods_port', 'ods_database', 'ods_password'
    for key, value in result.items():
        Variable.set(key, value)

update_variables = PythonOperator(
    task_id='update_variables',
    python_callable=update_ods_host_variables,
    provide_context=True,
)

pod_task_xcom_result >> update_variables
