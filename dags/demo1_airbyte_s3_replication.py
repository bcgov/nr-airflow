from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
import pendulum
import json

# For Oracle -> S3 replication of ORG_UNIT table
# API service: http://dev-release-ab-airbyte-api-server-svc 
# API route: https://nr-airbyte-api.apps.emerald.devops.gov.bc.ca
# Beware of competing resources when running airflow/airbyte at the same time

airbyte_job_type = "sync"
airbyte_connection_id = "46f2508b-0759-41c6-8f8b-33dd2910dd37"
airbyte_workspace_id = "c7528958-f674-4c2c-b91b-95030f0c4513"

with DAG(dag_id='demo1_airbyte_s3_replication',
         schedule=None,
         start_date=pendulum.today('UTC')
         ) as dag:
        demo2_airbyte_s3_replication = SimpleHttpOperator(
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
