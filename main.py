import psycopg2
import os
from github import Github


try:
    ODS_HOST = os.environ["ODS_HOST"]
    ODS_PORT = os.environ["ODS_PORT"]
    ODS_USER = os.environ["ODS_USER"]
    ODS_PASSWORD = os.environ["ODS_PASSWORD"]
    ODS_DB = os.environ["ODS_DB"]
    GH_TOKEN_SECRET = os.environ("GH_TOKEN_SECRET")

except KeyError:
    SOME_SECRET = "Token not available!"
    # or raise an error if it's not available so that the workflow fails

# Function to connect to PostgreSQL and scan for new applications
def scan_table_for_new_apps():
    conn = psycopg2.connect(
        host=ODS_HOST,
        port=ODS_PORT,
        dbname=ODS_DB,
        user=ODS_USER,
        password=ODS_PASSWORD
    )
    cursor = conn.cursor()

    cursor.execute("SELECT DISTINCT application_name FROM ods_data_management.cdc_master_table_list")
    applications = cursor.fetchall()

    cursor.close()
    conn.close()

    return applications

# Function to create a DAG definition
def create_dag_definition(application):
    dag_id = f"pipeline-{application}"
    app = application
    #LOB = application
    DAG = f""" 
from airflow import DAG
from pendulum import datetime
from kubernetes import client
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from datetime import timedelta

LOB = "{app}"
LOB = LOB.lower()

default_args = {{
    "email": ["NRM.DataFoundations@gov.bc.ca"],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry": True,
}}

with DAG(
    start_date=datetime(2023, 11, 23),
    catchup=False,
    schedule='5 12 * * *',
    dag_id=f"pipeline-{{LOB}}",
    default_args=default_args,
) as dag:
    ods_secrets = Secret("env", None, "ods-database")
    lob_secrets = Secret("env", None, f"{{LOB}}-database")

    run_replication = KubernetesPodOperator(
        task_id="run_replication",
        image="ghcr.io/bcgov/nr-dap-ods:main",
        image_pull_policy="Always",
        in_cluster=True,
        service_account_name="airflow-admin",
        name=f"run_{{LOB}}_replication",
        labels={{"DataClass": "Medium", "ConnectionType": "database",  "Release": "airflow"}},
        is_delete_operator_pod=False,
        secrets=[lob_secrets, ods_secrets],
        container_resources=client.V1ResourceRequirements(
            requests={{"cpu": "50m", "memory": "512Mi"}},
            limits={{"cpu": "100m", "memory": "1024Mi"}}
        )
    )"""
    #.format(app=application)
    #DAG = """{DAG}"""
    DAG = str(DAG)

    g = Github(GH_TOKEN_SECRET)
    repo = g.get_repo('bcgov/nr-airflow')

    repo.create_file(f'dags/{dag_id}.py', 'upload dags', DAG, branch='ui-testing')
    
    repo = g.get_repo("bcgov/nr-airflow")
    contents = repo.get_contents("/dags", ref="ui-testing")
    print([c for c in contents])
    for content_file in contents:
        print(content_file)
    return DAG


def extract_names(input_list):
    output_list = []
    prefix = "dags/pipeline-"
    suffix = ".py"
    for item in input_list:
        # Remove the prefix and suffix
        name = item[len(prefix):-len(suffix)]
        output_list.append(name)
    return output_list

# Main function to orchestrate the process
def main():
    g = Github(GH_TOKEN_SECRET)
    repo = g.get_repo("bcgov/nr-airflow")
    contents = repo.get_contents("/dags", ref="ui-testing")
    existing_dags= [c.path for c in contents]
    existing_dag_list = extract_names(existing_dags)
    print(existing_dag_list)

    #apps=scan_table_for_new_apps()
    #for app in apps:
        #dag_id = f"pipeline-{app[0]}"
        #existing_dags = set(dag_id)

    
    new_applications = scan_table_for_new_apps()

    for application in new_applications:
        application = application[0]  # Extract application name
        if application not in existing_dag_list:
            dag_content = create_dag_definition(application)
            print(dag_content)
            existing_dag_list.append(application)
            #push_to_github(dag_content, application)
            #existing_dags.add(application)

if __name__ == "__main__":
    main()
