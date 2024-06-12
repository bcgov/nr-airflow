from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import datetime
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'Data Foundations',
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "email": ["NRM.DataFoundations@gov.bc.ca"],
    "email_on_failure": True,
    "email_on_retry": True,
}


@dag(
    "lob_dq_ats_connectivity_hist",
    start_date=datetime(2024, 2, 20),
    max_active_runs=3,
    schedule=None,
    default_args=default_args,
    template_searchpath="/opt/bitnami/airflow/dags/git_nr-airflow-dags/include/",
    catchup=False,
    description='DAG to create monthly ATS connectivity history table'
)
def exe_ods_ats_hist_process():
    
    expire_old_records = PostgresOperator(
        task_id='execute_sql_expire_old_records',
        sql="ats_connectivity_expire_old_records.sql",
        postgres_conn_id="postgres_ods_conn",
        autocommit=True,
    )
    
    insert_new_changed_records = PostgresOperator(
        task_id='execute_sql_insert_new_changed_records',
        sql="ats_connectivity_insert_new_changed_records.sql",
        postgres_conn_id="postgres_ods_conn",
        autocommit=True,
    )

     update_flag_expired_records = PostgresOperator(
        task_id='execute_sql_update_flag_expired_records',
        sql="ats_connectivity_update_flag_expired_records.sql",
        postgres_conn_id="postgres_ods_conn",
        autocommit=True,
     )
    
    update_flag_closed_records = PostgresOperator(
        task_id='execute_sql_update_flag_closed_records',
        sql="ats_connectivity_update_flag_closed_records.sql",
        postgres_conn_id="postgres_ods_conn",
        autocommit=True,
    )

    archive_hist_records = PostgresOperator(
        task_id='execute_sql_archive_history_records',
        sql="ats_connectivity_archive_history_records.sql",
        postgres_conn_id="postgres_ods_conn",
    )    
    
    expire_old_records >> insert_new_changed_records >> update_flag_expired_records >> update_flag_closed_records >> archive_hist_records


exe_ods_ats_hist_process()
