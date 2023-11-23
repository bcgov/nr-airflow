from airflow import DAG
from pendulum import datetime
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from airflow.configuration import conf

namespace = conf.get("kubernetes", "NAMESPACE")

fta_config = {
    "init":{
	    "source_schema": "the",
        "target_schema": "fta_replication"
        },
    "sor_object":[
	      {"obj":"harvesting_authority","cdc_column":"update_timestamp"},
          {"obj":"prov_forest_use" ,"cdc_column":"update_timestamp"},
          {"obj":"file_type_code" ,"cdc_column":"update_timestamp"},
          {"obj":"harvest_type_code" ,"cdc_column":"update_timestamp"},
          {"obj":"harvest_auth_status_code" ,"cdc_column":"update_timestamp"},
          {"obj":"tenure_application_map_feature","cdc_column":"update_timestamp"},
          {"obj":"tenure_application" ,"cdc_column":"update_timestamp"},
          {"obj":"tenure_application_state_code" ,"cdc_column":"update_timestamp"},
          {"obj":"tenure_application_type_code" ,"cdc_column":"update_timestamp"}]
}

with DAG(
    start_date=datetime(2023, 11, 23),
    catchup=False,
    schedule="@daily",
    dag_id="openshift_pod",
) as dag:
    oc_run_replication_script = KubernetesPodOperator(
        task_id="run_replication_script",
        image="image-registry.openshift-image-registry.svc:5000/a1b9b0-dev/airflow-replication@sha256:8a6a236ad979becbd41bb8cd9f6c3103e59430b604e8e88fa7d67f9477563ad6",
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
        env_vars={"extract.json": f"{fta_config}"},
    )