from airflow import DAG
from airflow.providers.oracle.operators.oracle import OracleOperator
from airflow.utils.dates import datetime
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 23),
}

with DAG('oracle_example_print',
        default_args=default_args,
        schedule_interval=None) as dag:
    t1 = OracleOperator(
        task_id='execute_sql',
        sql="SELECT count(*) FROM THE.TENURE_APPLICATION_STATE_CODE",
        oracle_conn_id="oracle_fta_dev_conn",
        autocommit=True,
    )

    def print_result(**context):
        ti = context['task_instance']
        result = ti.xcom_pull(task_ids='execute_sql')
        print("Result of SQL query:")
        for row in result:
            print(row)

    t2 = PythonOperator(
        task_id='print_result',
        python_callable=print_result,
        provide_context=True,
    )

    t1 >> t2
