from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import datetime
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 23),
    # Add other necessary default arguments
}

with DAG('postgres_example_print', default_args=default_args, schedule_interval=None) as dag:
    t1 = PostgresOperator(
        task_id='execute_sql',
        sql="SELECT count(*) FROM fta_replication.tenure_application_state_code",
        postgres_conn_id="postgres_ods_conn",  # Update connection ID
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
