from airflow import DAG
from pendulum import datetime
from kubernetes import client
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator

ods_password = Variable.get("ods_password")
dlh_password = Variable.get("dlh_password")

with DAG(
    start_date=datetime(2024, 2, 13),
    catchup=False,
    schedule=None,
    dag_id="pmt_dlh_load_facts_dims",
) as dag:
    start_task = DummyOperator(task_id="start_task")
    
    load_dim_org = KubernetesPodOperator(
        task_id="task1_load_dim_org",      
        image="ghcr.io/bcgov/nr-dap-dlh-pmt:main",
        image_pull_policy="Always",
        in_cluster=True,
        namespace="a1b9b0-dev",
        service_account_name="airflow-admin",
        name="task1_load_dim_org",
        random_name_suffix=True,
        labels={"DataClass": "Low", "env": "dev", "ConnectionType": "database"},
        env_vars={"ods_password": ods_password, "dlh_password": dlh_password},
        reattach_on_restart=True,
        is_delete_operator_pod=False,
        get_logs=True,
        log_events_on_failure=True,
        container_resources= client.V1ResourceRequirements(
        requests={"cpu": "50m", "memory": "256Mi"},
        limits={"cpu": "1", "memory": "1Gi"}),
        cmds=["dbt"], 
        arguments=["snapshot","--select","dim_org","--profiles-dir","/usr/app/dbt/.dbt"]
        # arguments=["test","--profiles-dir","/usr/app/dbt/.dbt"]
    )
    
    load_dim_authorization_status = KubernetesPodOperator(
        task_id="task2_load_dim_authorization_status",      
        image="ghcr.io/bcgov/nr-dap-dlh-pmt:main",
        image_pull_policy="Always",
        in_cluster=True,
        namespace="a1b9b0-dev",
        service_account_name="airflow-admin",
        name="task2_load_dim_authorization_status",
        random_name_suffix=True,
        labels={"DataClass": "Low", "env": "dev", "ConnectionType": "database"},
        env_vars={"ods_password": ods_password, "dlh_password": dlh_password},
        reattach_on_restart=True,
        is_delete_operator_pod=False,
        get_logs=True,
        log_events_on_failure=True,
        container_resources= client.V1ResourceRequirements(
        requests={"cpu": "50m", "memory": "256Mi"},
        limits={"cpu": "1", "memory": "1Gi"}),
        cmds=["dbt"], 
        arguments=["snapshot","--select","dim_authorization_status","--profiles-dir","/usr/app/dbt/.dbt"]
        # arguments=["test","--profiles-dir","/usr/app/dbt/.dbt"]
    )
    
    load_dim_business_area = KubernetesPodOperator(
        task_id="task3_load_dim_business_area",      
        image="ghcr.io/bcgov/nr-dap-dlh-pmt:main",
        image_pull_policy="Always",
        in_cluster=True,
        namespace="a1b9b0-dev",
        service_account_name="airflow-admin",
        name="task3_dim_business_area",
        random_name_suffix=True,
        labels={"DataClass": "Low", "env": "dev", "ConnectionType": "database"},
        env_vars={"ods_password": ods_password, "dlh_password": dlh_password},
        reattach_on_restart=True,
        is_delete_operator_pod=False,
        get_logs=True,
        log_events_on_failure=True,
        container_resources= client.V1ResourceRequirements(
        requests={"cpu": "50m", "memory": "256Mi"},
        limits={"cpu": "1", "memory": "1Gi"}),
        cmds=["dbt"], 
        arguments=["snapshot","--select","dim_business_area","--profiles-dir","/usr/app/dbt/.dbt"]
        # arguments=["test","--profiles-dir","/usr/app/dbt/.dbt"]
    )
    
    load_dim_location = KubernetesPodOperator(
        task_id="task4_load_dim_location",      
        image="ghcr.io/bcgov/nr-dap-dlh-pmt:main",
        image_pull_policy="Always",
        in_cluster=True,
        namespace="a1b9b0-dev",
        service_account_name="airflow-admin",
        name="task4_load_dim_location",
        random_name_suffix=True,
        labels={"DataClass": "Low", "env": "dev", "ConnectionType": "database"},
        env_vars={"ods_password": ods_password, "dlh_password": dlh_password},
        reattach_on_restart=True,
        is_delete_operator_pod=False,
        get_logs=True,
        log_events_on_failure=True,
        container_resources= client.V1ResourceRequirements(
        requests={"cpu": "50m", "memory": "256Mi"},
        limits={"cpu": "1", "memory": "1Gi"}),
        cmds=["dbt"], 
        arguments=["snapshot","--select","dim_location","--profiles-dir","/usr/app/dbt/.dbt"]
        # arguments=["test","--profiles-dir","/usr/app/dbt/.dbt"]
    )
    
    load_dim_ministry = KubernetesPodOperator(
        task_id="task5_load_dim_ministry",      
        image="ghcr.io/bcgov/nr-dap-dlh-pmt:main",
        image_pull_policy="Always",
        in_cluster=True,
        namespace="a1b9b0-dev",
        service_account_name="airflow-admin",
        name="task5_load_dim_ministry",
        random_name_suffix=True,
        labels={"DataClass": "Low", "env": "dev", "ConnectionType": "database"},
        env_vars={"ods_password": ods_password, "dlh_password": dlh_password},
        reattach_on_restart=True,
        is_delete_operator_pod=False,
        get_logs=True,
        log_events_on_failure=True,
        container_resources= client.V1ResourceRequirements(
        requests={"cpu": "50m", "memory": "256Mi"},
        limits={"cpu": "1", "memory": "1Gi"}),
        cmds=["dbt"], 
        arguments=["snapshot","--select","dim_ministry","--profiles-dir","/usr/app/dbt/.dbt"]
        # arguments=["test","--profiles-dir","/usr/app/dbt/.dbt"]
    )
    
    load_dim_permit_status = KubernetesPodOperator(
        task_id="task6_load_dim_permit_status",      
        image="ghcr.io/bcgov/nr-dap-dlh-pmt:main",
        image_pull_policy="Always",
        in_cluster=True,
        namespace="a1b9b0-dev",
        service_account_name="airflow-admin",
        name="task5_load_dim_permit_status",
        random_name_suffix=True,
        labels={"DataClass": "Low", "env": "dev", "ConnectionType": "database"},
        env_vars={"ods_password": ods_password, "dlh_password": dlh_password},
        reattach_on_restart=True,
        is_delete_operator_pod=False,
        get_logs=True,
        log_events_on_failure=True,
        container_resources= client.V1ResourceRequirements(
        requests={"cpu": "50m", "memory": "256Mi"},
        limits={"cpu": "1", "memory": "1Gi"}),
        cmds=["dbt"], 
        arguments=["snapshot","--select","dim_permit_status","--profiles-dir","/usr/app/dbt/.dbt"]
        # arguments=["test","--profiles-dir","/usr/app/dbt/.dbt"]
    )
    
    load_dim_permit_type = KubernetesPodOperator(
        task_id="task7_load_dim_permit_type",      
        image="ghcr.io/bcgov/nr-dap-dlh-pmt:main",
        image_pull_policy="Always",
        in_cluster=True,
        namespace="a1b9b0-dev",
        service_account_name="airflow-admin",
        name="task7_load_dim_permit_type",
        random_name_suffix=True,
        labels={"DataClass": "Low", "env": "dev", "ConnectionType": "database"},
        env_vars={"ods_password": ods_password, "dlh_password": dlh_password},
        reattach_on_restart=True,
        is_delete_operator_pod=False,
        get_logs=True,
        log_events_on_failure=True,
        container_resources= client.V1ResourceRequirements(
        requests={"cpu": "50m", "memory": "256Mi"},
        limits={"cpu": "1", "memory": "1Gi"}),
        cmds=["dbt"], 
        arguments=["snapshot","--select","dim_permit_type","--profiles-dir","/usr/app/dbt/.dbt"]
        # arguments=["test","--profiles-dir","/usr/app/dbt/.dbt"]
    )
    
    load_dim_project = KubernetesPodOperator(
        task_id="task8_load_dim_project",      
        image="ghcr.io/bcgov/nr-dap-dlh-pmt:main",
        image_pull_policy="Always",
        in_cluster=True,
        namespace="a1b9b0-dev",
        service_account_name="airflow-admin",
        name="task8_load_dim_project",
        random_name_suffix=True,
        labels={"DataClass": "Low", "env": "dev", "ConnectionType": "database"},
        env_vars={"ods_password": ods_password, "dlh_password": dlh_password},
        reattach_on_restart=True,
        is_delete_operator_pod=False,
        get_logs=True,
        log_events_on_failure=True,
        container_resources= client.V1ResourceRequirements(
        requests={"cpu": "50m", "memory": "256Mi"},
        limits={"cpu": "1", "memory": "1Gi"}),
        cmds=["dbt"], 
        arguments=["snapshot","--select","dim_project","--profiles-dir","/usr/app/dbt/.dbt"]
        # arguments=["test","--profiles-dir","/usr/app/dbt/.dbt"]
    )
    
    load_dim_source_system = KubernetesPodOperator(
        task_id="task9_load_dim_source_system",      
        image="ghcr.io/bcgov/nr-dap-dlh-pmt:main",
        image_pull_policy="Always",
        in_cluster=True,
        namespace="a1b9b0-dev",
        service_account_name="airflow-admin",
        name="task9_load_dim_source_system",
        random_name_suffix=True,
        labels={"DataClass": "Low", "env": "dev", "ConnectionType": "database"},
        env_vars={"ods_password": ods_password, "dlh_password": dlh_password},
        reattach_on_restart=True,
        is_delete_operator_pod=False,
        get_logs=True,
        log_events_on_failure=True,
        container_resources= client.V1ResourceRequirements(
        requests={"cpu": "50m", "memory": "256Mi"},
        limits={"cpu": "1", "memory": "1Gi"}),
        cmds=["dbt"], 
        arguments=["snapshot","--select","dim_source_system","--profiles-dir","/usr/app/dbt/.dbt"]
        # arguments=["test","--profiles-dir","/usr/app/dbt/.dbt"]
    )
    
    load_fact_permits = KubernetesPodOperator(
        task_id="task10_load_fact_permits",      
        image="ghcr.io/bcgov/nr-dap-dlh-pmt:main",
        image_pull_policy="Always",
        in_cluster=True,
        namespace="a1b9b0-dev",
        service_account_name="airflow-admin",
        name="task10_load_fact_permits",
        random_name_suffix=True,
        labels={"DataClass": "Low", "env": "dev", "ConnectionType": "database"},
        env_vars={"ods_password": ods_password, "dlh_password": dlh_password},
        reattach_on_restart=True,
        is_delete_operator_pod=False,
        get_logs=True,
        log_events_on_failure=True,
        container_resources= client.V1ResourceRequirements(
        requests={"cpu": "50m", "memory": "256Mi"},
        limits={"cpu": "1", "memory": "1Gi"}),
        cmds=["dbt"], 
        arguments=["snapshot","--select","fact_permits","--profiles-dir","/usr/app/dbt/.dbt"]
        # arguments=["test","--profiles-dir","/usr/app/dbt/.dbt"]
    )
    
    # Set task dependencies
    start_task >> [load_dim_org, load_dim_authorization_status, load_dim_business_area, load_dim_location, load_dim_ministry, load_dim_permit_status, load_dim_permit_type, load_dim_project, load_dim_source_system] >> load_fact_permits
    
    