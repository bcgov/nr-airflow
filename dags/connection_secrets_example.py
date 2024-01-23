from airflow import DAG
from pendulum import datetime
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

def print_connection_details(**kwargs):
    fta_connection = BaseHook.get_connection('oracle_fta_dev_conn')

    conn_type = fta_connection.conn_type
    host = fta_connection.host
    database = fta_connection.schema
    username = fta_connection.login
    password = fta_connection.password
    port = fta_connection.port

    print(conn_type, host, database, username, password, port)

with DAG(
    start_date=datetime(2023, 11, 30),
    catchup=False,
    schedule=None,
    dag_id="connection_secrets_example",
) as dag:
    python_task = PythonOperator(
        task_id='print_connection_details',
        python_callable=print_connection_details,
        provide_context=True,
        dag=dag,
    )
