# Airflow Set Up for NRM Data Analytics Platform

Airflow is a central platform to view, schedule, and monitor jobs, and the primary ETL/ELT job orchestrator within the DAP. Airflow is configured to use the Kubernetes executor. When an Airflow job is triggered, a pod is dynamically spun up in OpenShift. This pod can contain one or more containers, which enables the use of any data processing language or tool (e.g. DBT, Python, R). The preferred pattern for creating Airflow DAGs is to use the KubernetesOperator where possible – allowing for refined control of the container environment for each job. Other operators such as the PythonOperator, PostgresOperator, and BashOperator are supported depending use case. 

Airflow is deployed in the Emerald cluster of the BCGov OpenShift Container Platform. Deployment details below.

## Deploying Airflow to OpenShift

Prerequisites:
- OpenShift CLI
- Helm CLI

Create OpenShift ConfigMap for requirements.txt:
```sh
oc create configmap airflow-requirements --from-file=requirements.txt
```
Navigate to the 'oc' folder then:
```sh
oc apply -f .
```
Navigate to the 'airflow' folder then:
```sh
helm install airflow .
```

## Helm chart sourced from
```sh
helm pull oci://registry-1.docker.io/bitnamicharts/airflow
```

## Visit the application here:
http://nr-airflow.apps.emerald.devops.gov.bc.ca/

## Upgrade OpenShift Deployment
```sh 
helm upgrade -f values.yaml airflow .
```
