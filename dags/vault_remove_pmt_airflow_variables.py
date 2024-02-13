from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from pendulum import datetime

def remove_pmt_variables():
    variable = Variable.get('pmt_all_creds')
    # Convert string to list
    variable_list = eval(variable)
    for v in variable_list:
        try:
            Variable.delete(v)
        except:
            print(f"Failed to delete the {v} variable.")
    Variable.delete('pmt_all_creds')

with DAG(
    start_date=datetime(2023, 11, 23),
    catchup=False,
    schedule_interval='@daily',
    dag_id="vault_remove_pmt_airflow_variables",
) as dag:

    remove_pmt_variables_task = PythonOperator(
        task_id="remove_pmt_variables_task",
        python_callable=remove_pmt_variables
    )

remove_pmt_variables_task
