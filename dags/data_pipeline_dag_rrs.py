from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from data_replication_cls import data_replication

dag = DAG('data_pipeline_dag_rrs', description='Data Pipeline to execute replication and ETL for RRS tables', schedule_interval='0 12 * * *', start_date=datetime(2017, 3, 20), catchup=False)

with dag:
 dummy_task = DummyOperator(task_id='ETL_Start', retries = 3),

 start_replication_rrs = data_replication(mstr_schema = 'app_rrs1', 
                                          app_name = 'rrs', 
                                          env = 'dev',
                                          task_id='start_replication_rrs'
                                          )

dummy_task >> start_replication_rrs