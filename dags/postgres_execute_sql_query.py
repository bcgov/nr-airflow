from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import datetime
from airflow.operators.python_operator import PythonOperator


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    "call_snowflake_sprocs",
    start_date=datetime(2023, 8, 23),
    max_active_runs=3,
    schedule="@daily",
    default_args=default_args,
    template_searchpath="/dags/nr-airflow-dags/include",
    catchup=False,
)
def call_snowflake_sprocs():
    opr_call_sproc1 = SQLExecuteQueryOperator(
        task_id="execute_sql", conn_id="postgres_ods_conn", sql="pmt_tenure_app_state_code_test.sql"
    )

    opr_call_sproc2 = SQLExecuteQueryOperator(
        task_id="execute_sql2", conn_id="postgres_ods_conn", sql="pmt_tenure_app_state_code_test.sql"
    )

    opr_call_sproc1 >> opr_call_sproc2


call_snowflake_sprocs()
