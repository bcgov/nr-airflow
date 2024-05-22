from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta

# Define default_args dictionary
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 11, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the controller DAG
controller_dag = DAG(
    'lob_dq_ats_controller',
    default_args=default_args,
    description='ATS Controller DAG to run other DAGs in order',
    schedule_interval=None,  # Set your desired schedule_interval
    catchup=False,  # Set to False to skip historical runs
)

# Define the list of sub-DAGs in the desired order
sub_dags_in_order = [
    'lob_dq_ats_housing',
    'lob_dq_ats_housing_hist',
    'lob_dq_ats_connectivity',
    'lob_dq_ats_connectivity_hist'
]

# Create TriggerDagRunOperator for each sub-DAG
trigger_operators = {}
for sub_dag_id in sub_dags_in_order:
    trigger_operator = TriggerDagRunOperator(
        task_id=f'trigger_{sub_dag_id}',
        trigger_dag_id=sub_dag_id,
        # conf={'batch_id': 'your_batch_id_value'},  # Pass any necessary configuration
        dag=controller_dag,
    )
    trigger_operators[sub_dag_id] = trigger_operator

# Set up task dependencies
trigger_operators['lob_dq_ats_housing'] >> trigger_operators['lob_dq_ats_housing_hist'] 
trigger_operators['lob_dq_ats_connectivity'] >> trigger_operators['lob_dq_ats_connectivity_hist']  

