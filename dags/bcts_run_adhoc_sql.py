"""

This DAG is used to run sql scripts as bcts_etl_user, which has admin privilleges. 
This is used to 
    1. Add new tables to ods_data_management.cdc_master_table_list
    2. Create new tables in the replication schema
    3. Fix any data inaccuracies after proper approval process (JIRA ticket, approval from BCTS)
    4. Update any schema changes from source system

 How to use?
   1. Add the sql files to nr-dap-ods\shared\bcts_adhoc_sql\sql\active
   2. Trigger this DAG manually and pass the sql file name as an argument to execute the ddl scripts in ODS
 
   Manual Trigger Parameter:
    file_names: bcts_lrm_forest_ddl.sql, cdc_master_table_list.sql

"""

from airflow import DAG
from pendulum import datetime
from kubernetes import client
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.decorators import task
from airflow.models.param import Param
from datetime import timedelta
import os

LOB = 'lrm'
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
    schedule=None,
    dag_id=f"bcts_run_adhoc_sql",
    default_args=default_args,
    description='DAG to to run sql scripts as bcts_etl_user',
    params={"file_names": Param([], type="array")},
) as dag:
    
    if ENV == 'LOCAL':\
        run_replication = KubernetesPodOperator(
            task_id="bcts_run_adhoc_sql",
            image="nrids-bcts-run-sql-ods:main",
            cmds=["python3", "./run_sql.py"],
            arguments=['{{ dag_run.conf["file_names"]}}'],
            # Following configs are different in the local development environment
            # image_pull_policy="Always",
            # in_cluster=True,
            # service_account_name="airflow-admin",
            name=f"run_adhoc_sql_ODS_for_BCTS",
            labels={"DataClass": "Medium", "ConnectionType": "database",  "Release": "airflow"},
            is_delete_operator_pod=True,
            secrets=[ods_secrets],
            container_resources= client.V1ResourceRequirements(
            requests={"cpu": "50m", "memory": "512Mi"},
            limits={"cpu": "100m", "memory": "1024Mi"})
        )
    else:
        # In Dev, Test, and Prod Environments
        run_replication = KubernetesPodOperator(
            task_id="bcts_run_adhoc_sql",
            image="ghcr.io/bcgov/nr-dap-ods-run-sql-ods:main",
            cmds=["python3", "./run_sql.py"],
            arguments=['{{dag_run.conf["file_names"]}}'],
            image_pull_policy="Always",
            in_cluster=True,
            service_account_name="airflow-admin",
            name=f"run_adhoc_sql_ODS_for_BCTS",
            labels={"DataClass": "Medium", "ConnectionType": "database",  "Release": "airflow"},
            is_delete_operator_pod=True,
            secrets=[ods_secrets],
            container_resources= client.V1ResourceRequirements(
            requests={"cpu": "50m", "memory": "512Mi"},
            limits={"cpu": "100m", "memory": "1024Mi"}),
            random_name_suffix=False
        )