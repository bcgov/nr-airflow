from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from pendulum import datetime

def remove_pmt_variables():
    all_variables = Variable.get(xcom=False)
    for key in all_variables:
        if key.startswith('pmt_'):
            Variable.delete(key)

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
