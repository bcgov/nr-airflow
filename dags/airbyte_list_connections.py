from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
import pendulum

# API is located at http://dev-release-ab-airbyte-api-server-svc 

airbyte_job_type = "sync"
airbyte_connection_id = "2b9f5022-51d3-4fdf-ba83-82d1b5d80ce8"
airbyte_workspace_id = "ea2db1ec-0357-4868-b216-4ca7333f5df4"

with DAG(dag_id='airbyte_list_connections',
         schedule=None,
         start_date=pendulum.today('UTC')
         ) as dag:
        airbyte_connections = SimpleHttpOperator(
                method="GET",
                task_id='get_airbyte_connections',
                http_conn_id='airbyte-api',
                headers={"Accept": "application/json"},
                endpoint= f'/v1/connections?workspaceIds={airbyte_workspace_id}&includeDeleted=false',
                log_response=True)