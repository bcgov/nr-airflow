from airflow import DAG
from pendulum import datetime
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

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
    schedule=None,
    dag_id="openshift_pod",
) as dag:
    oc_run_replication_script = KubernetesPodOperator(
        task_id="run_replication_script",
        # need to add ODS and FTA secrets here then swap image
        image="image-registry.openshift-image-registry.svc:5000/a1b9b0-dev/fta-replication-airflow@sha256:854bff78aeb3580224664b7e743884126e38aea8516f46713276145eee7dc96b",
        in_cluster=True,
        namespace="a1b9b0-dev",
        name="oc_run_replication_script",
        # give the Pod name a random suffix, ensure uniqueness in the namespace
        random_name_suffix=True,
        labels={"DataClass": "Medium", "env": "dev"},
        # reattach to worker instead of creating a new Pod on worker failure
        reattach_on_restart=True,
        # delete Pod after the task is finished
        is_delete_operator_pod=False,
        get_logs=True,
        log_events_on_failure=True,
        # Not using this right now, just testing it out
        env_vars={"extract.json": f"{fta_config}"}
        # container_resources=None,
        # service_account_name="airflow-admin",
        # configmaps=["fta-pipeline"]
        # secrets=["ods-database", "fta-database"]
    )