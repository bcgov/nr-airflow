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
    start_date=datetime(2024, 2, 13),
    catchup=False,
    schedule=None,
    dag_id="vault_update_dlh_ods_af_vars",
) as dag:
    admin_ods_dev_vault_action = KubernetesPodOperator(
        task_id="get_admin_ods_dev",
        image="ghcr.io/bcgov/nr-vault-patterns:main",
        image_pull_policy="Always",
        in_cluster=True,
        namespace="a1b9b0-dev",
        name="get_admin_ods_dev",
        random_name_suffix=True,
        labels={"DataClass": "High", "Release": "test-release-af"},  # network policies
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
    
    admin_dlh_dev_vault_action = KubernetesPodOperator(
        task_id="get_admin_dlh_dev",
        image="ghcr.io/bcgov/nr-vault-patterns:main",
        image_pull_policy="Always",
        in_cluster=True,
        namespace="a1b9b0-dev",
        name="get_admin_dlh_dev",
        random_name_suffix=True,
        labels={"DataClass": "High", "Release": "test-release-af"},  # network policies
        reattach_on_restart=True,
        is_delete_operator_pod=False,
        get_logs=True,
        log_events_on_failure=True,
        secrets=[vault_jwt],
        env_vars={"VAULT_ENV": "dev", "SECRET_NAME": "admin-dlh-dev"}, 
        do_xcom_push=True, # allows pushing the secrets to return.json
        container_resources= client.V1ResourceRequirements(
        requests={"cpu": "10m", "memory": "256Mi"},
        limits={"cpu": "50m", "memory": "500Mi"})
    )

    def update_ods_host_variables(task_instance,task_ids, **kwargs):
        results = {}
        for task_id in task_ids:
            result = task_instance.xcom_pull(task_ids=task_id)
            results.update(result)

        for key, value in results.items():
            Variable.set(key, value)    

    update_variables_ods = PythonOperator(
        task_id='update_variables_ods',
        python_callable=update_ods_host_variables,
        op_kwargs={'task_ids': ['get_admin_ods_dev']},
        provide_context=True,
    )
    
    update_variables_dlh = PythonOperator(
        task_id='update_variables_dlh',
        python_callable=update_ods_host_variables,
        op_kwargs={'task_ids': ['get_admin_dlh_dev']},
        provide_context=True,
    )    

admin_ods_dev_vault_action >> admin_dlh_dev_vault_action >> update_variables_ods >> update_variables_dlh
