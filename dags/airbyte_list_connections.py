from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
import pendulum

airbyte_workspace_id = "c7528958-f674-4c2c-b91b-95030f0c4513"

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
