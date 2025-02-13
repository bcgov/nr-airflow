from airflow import DAG
from datetime import datetime, timezone
from kubernetes import client
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.dummy import DummyOperator
from datetime import timedelta
import os

LOB = 'lrm'
annual_developed_volume_transformation_sql_file_path = './Annual_Developed_Volume_Query.sql'
# For local development environment only.
ENV = os.getenv("AIRFLOW_ENV")

ods_secrets = Secret("env", None, f"{LOB}-ods-database")

if ENV == 'LOCAL':
    default_args = {
        'owner': 'BCTS',
        "email": ["sreejith.munthikodu@gov.bc.ca"],
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        "email_on_failure": False, # No alerts in local environment
        "email_on_retry": False,
    }
else:
    default_args = {
        'owner': 'BCTS',
        "email": ["sreejith.munthikodu@gov.bc.ca"],
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        "email_on_failure": True,
        "email_on_retry": False,
    }

with DAG(
    start_date=datetime(2024, 10, 23),
    catchup=False,
    schedule='45 12 * * MON-FRI',
    dag_id=f"bcts_transformations",
    default_args=default_args,
    description='DAG to run the transformations in ODS for BCTS Annual Developed Volume Dashboard',
) as dag:
    
    wait_for_lrm_replication = ExternalTaskSensor(
        task_id='wait_for_lrm_replication',
        external_dag_id='bcts-replication-lrm',
        external_task_id='task_completion_flag',
        timeout=60000,  # Timeout in seconds
        poke_interval=30,  # How often to check (in seconds)
        execution_delta = timedelta(minutes=15)
    )

    wait_for_bctsadmin_replication = ExternalTaskSensor(
        task_id='wait_for_bctsadmin_replication',
        external_dag_id='bcts-replication-bctsadmin',
        external_task_id='task_completion_flag',
        timeout=60000,  # Timeout in seconds
        poke_interval=30,  # How often to check (in seconds)
        execution_delta = timedelta(minutes=40)
    )

    wait_for_bcts_client_replication = ExternalTaskSensor(
        task_id='wait_for_bcts_client_replication',
        external_dag_id='bcts-replication-client',
        external_task_id='task_completion_flag',
        timeout=60000,  # Timeout in seconds
        poke_interval=30,  # How often to check (in seconds)
        execution_delta = timedelta(minutes=35)
    )
    
    if ENV == 'LOCAL':

        annual_developed_volume_transformation = KubernetesPodOperator(
            task_id="annual_developed_volume_transformation",
            image="nrids-bcts-data-pg-transformations:main",
            cmds=["python3", "./bcts_etl.py"],
            arguments=[annual_developed_volume_transformation_sql_file_path],
            # Following configs are different in the local development environment
            # image_pull_policy="Always",
            # in_cluster=True,
            # service_account_name="airflow-admin",
            name=f"run_{LOB}_transformation_annual_developed_volume",
            labels={"DataClass": "Medium", "ConnectionType": "database",  "Release": "airflow"},
            is_delete_operator_pod=True,
            secrets=[ods_secrets],
            container_resources= client.V1ResourceRequirements(
            requests={"cpu": "50m", "memory": "512Mi"},
            limits={"cpu": "100m", "memory": "1024Mi"})
        )
    else:
        # In Dev, Test, and Prod Environments
        annual_developed_volume_transformation = KubernetesPodOperator(
            task_id="annual_developed_volume_transformation",
            image="ghcr.io/bcgov/nr-dap-ods-bctstransformations:main",
            cmds=["python3", "./bcts_etl.py"],
            arguments=[annual_developed_volume_transformation_sql_file_path],
            image_pull_policy="Always",
            in_cluster=True,
            service_account_name="airflow-admin",
            name=f"run_{LOB}_transformation_annual_developed_volume",
            labels={"DataClass": "Medium", "ConnectionType": "database",  "Release": "airflow"},
            is_delete_operator_pod=True,
            secrets=[ods_secrets],
            container_resources= client.V1ResourceRequirements(
            requests={"cpu": "50m", "memory": "512Mi"},
            limits={"cpu": "100m", "memory": "1024Mi"}),
            random_name_suffix=False
        )


        bcts_performance_report_transformation = KubernetesPodOperator(
            task_id="bcts_performance_report_transformation",
            image="ghcr.io/bcgov/nr-dap-ods-bctstransformations:main",
            cmds=["python3", "./bcts_performance_report_transformation.py"],
            image_pull_policy="Always",
            in_cluster=True,
            service_account_name="airflow-admin",
            name=f"run_{LOB}_transformation_bcts_performance_report",
            labels={"DataClass": "Medium", "ConnectionType": "database",  "Release": "airflow"},
            is_delete_operator_pod=True,
            secrets=[ods_secrets],
            container_resources= client.V1ResourceRequirements(
            requests={"cpu": "50m", "memory": "512Mi"},
            limits={"cpu": "100m", "memory": "1024Mi"}),
            random_name_suffix=False
        )

        bcts_timber_inventory_ready_to_sell_report_transformation = KubernetesPodOperator(
            task_id="bcts_timber_inventory_ready_to_sell_report_transformation",
            image="ghcr.io/bcgov/nr-dap-ods-bctstransformations:main",
            cmds=["python3", "./bcts_timber_inventory_ready_to_sell_transformation.py"],
            image_pull_policy="Always",
            in_cluster=True,
            service_account_name="airflow-admin",
            name=f"run_{LOB}_timber_inventory_ready_to_sell",
            labels={"DataClass": "Medium", "ConnectionType": "database",  "Release": "airflow"},
            is_delete_operator_pod=True,
            secrets=[ods_secrets],
            container_resources= client.V1ResourceRequirements(
            requests={"cpu": "50m", "memory": "512Mi"},
            limits={"cpu": "100m", "memory": "1024Mi"}),
            random_name_suffix=False
        )

    task_completion_flag = DummyOperator(
        task_id='task_completion_flag'
    )

    wait_for_lrm_replication >> annual_developed_volume_transformation 
    wait_for_lrm_replication >> bcts_performance_report_transformation
    wait_for_lrm_replication >> bcts_timber_inventory_ready_to_sell_report_transformation
    wait_for_bctsadmin_replication >> bcts_performance_report_transformation
    wait_for_bcts_client_replication >> bcts_performance_report_transformation
    
    annual_developed_volume_transformation >> task_completion_flag
    bcts_performance_report_transformation >> task_completion_flag
    bcts_timber_inventory_ready_to_sell_report_transformation >> task_completion_flag


    