from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta

# Define default_args dictionary
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 11, 27),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the controller DAG
controller_dag = DAG(
    'permitting_pipeline_controller',
    default_args=default_args,
    description='Controller DAG to run other DAGs in order',
    schedule_interval=None,  # Set your desired schedule_interval
    catchup=False,  # Set to False to skip historical runs
)

# Define the list of sub-DAGs in the desired order
sub_dags_in_order = [
    'permitting_pipeline_etl_batch_id_creation',
    'permitting_pipeline_fta',
    'permitting_pipeline_rrs',
    'permitting_pipeline_ats',
    'permitting_pipeline_etl_batch_id_update',
]

# Create TriggerDagRunOperator for each sub-DAG
for sub_dag_id in sub_dags_in_order:
    trigger_operator = TriggerDagRunOperator(
        task_id=f'trigger_{sub_dag_id}',
        trigger_dag_id=sub_dag_id,
        #conf={'batch_id': 'your_batch_id_value'},  # Pass any necessary configuration
        dag=controller_dag,
    )

    # Set up task dependencies
    if sub_dag_id == 'permitting_pipeline_etl_batch_id_creation':
        controller_dag >> trigger_operator
    elif sub_dag_id == 'permitting_pipeline_fta':
        controller_dag >> trigger_operator
    elif sub_dag_id == 'permitting_pipeline_rrs':
        controller_dag >> trigger_operator
    elif sub_dag_id == 'permitting_pipeline_ats':
        controller_dag >> trigger_operator
    elif sub_dag_id == 'permitting_pipeline_etl_batch_id_update':
        controller_dag.get_task('trigger_permitting_pipeline_fta') >> trigger_operator
        controller_dag.get_task('trigger_permitting_pipeline_rrs') >> trigger_operator
        controller_dag.get_task('trigger_permitting_pipeline_ats') >> trigger_operator
    else:
        controller_dag >> trigger_operator

# Define the order of task dependencies
