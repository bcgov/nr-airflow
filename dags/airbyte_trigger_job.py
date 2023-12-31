from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
import pendulum
import json

# API is located at http://dev-release-ab-airbyte-api-server-svc 
# job trigger sometimes does not working due to competing resources when running airflow/airbyte at the same time

airbyte_job_type = "sync"
airbyte_connection_id = "2b9f5022-51d3-4fdf-ba83-82d1b5d80ce8"
airbyte_workspace_id = "ea2db1ec-0357-4868-b216-4ca7333f5df4"

with DAG(dag_id='airbyte_trigger_job',
         schedule=None,
         start_date=pendulum.today('UTC')
         ) as dag:
        trigger_airbyte_sync = SimpleHttpOperator(
                method="POST",
                task_id='airbyte_sync',
                http_conn_id='airbyte-api',
                headers={
                "Content-Type":"application/json",
                "Accept": "application/json",
                "Content-Length": "72"},
                endpoint='/v1/jobs',
                data=json.dumps({"connectionId": airbyte_connection_id, "jobType": airbyte_job_type}),
                log_response=True)    
    
