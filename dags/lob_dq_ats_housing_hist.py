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
    "retry_delay": timedelta(minutes=1),
}


@dag(
    "postges_execute_sql_query_ods",
    start_date=datetime(2024, 2, 20),
    max_active_runs=3,
    schedule="@daily",
    default_args=default_args,
    template_searchpath="/opt/bitnami/airflow/dags/git_nr-airflow-dags/include/",
    catchup=False,
)
def exe_ods_ats_hist_process():
    
    expire_old_records = PostgresOperator(
        task_id='execute_sql_expire_old_records',
        sql="ats_housing_expire_old_records.sql",
        postgres_conn_id="postgres_ods_conn",  # Update connection ID
        autocommit=True,
    )
    
    insert_new_changed_records = PostgresOperator(
        task_id='execute_sql_insert_new_changed_records',
        sql="ats_housing_insert_new_changed_records.sql",
        postgres_conn_id="postgres_ods_conn",  # Update connection ID
        autocommit=True,
    )
    
    update_flag_closed_records = PostgresOperator(
        task_id='execute_sql_update_flag_closed_records',
        sql="ats_hosuing_update_flag_closed_records.sql",
        postgres_conn_id="postgres_ods_conn",  # Update connection ID
        autocommit=True,
    )
    
    expire_old_records >> insert_new_changed_records >> update_flag_closed_records


exe_ods_ats_hist_process()
