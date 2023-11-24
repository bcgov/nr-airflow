from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.models import Variable
import pendulum
import json

# API is located at http://dev-release-ab-airbyte-api-server-svc 

airbyte_job_type = "sync"
airbyte_connection_id = "2b9f5022-51d3-4fdf-ba83-82d1b5d80ce8"
airbyte_workspace_id = "ea2db1ec-0357-4868-b216-4ca7333f5df4"

with DAG(dag_id='airbyte_example',
         schedule=None,
         start_date=pendulum.today('UTC')
         ) as dag:
    airbyte_connections = SimpleHttpOperator(
            method="GET",
            task_id='get_airbyte_connections',
            http_conn_id='airbyte-api',
            headers={"Accept": "application/json"},
            endpoint= f'/v1/connections?workspaceIds={airbyte_workspace_id}&includeDeleted=false',
            log_response=True    
    )
    
# job trigger not working due to resource issues when running airflow/airbyte at the same time
    
# trigger_airbyte_sync = SimpleHttpOperator(
#        method="POST",
#        task_id='start_airbyte_sync',
#        http_conn_id='airbyte-api-cloud-connection',
#        headers={
#            "Content-Type":"application/json",
#            "Accept": "application/json",
#            "Content-Length": "72"},
#        endpoint='/v1/jobs',
#        data=json.dumps({"connectionId": airbyte_connection_id, "jobType": airbyte_job_type}),
#        log_response=True)