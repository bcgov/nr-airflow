# Airflow Set Up

## Helm chart sourced from
```sh
helm pull oci://registry-1.docker.io/bitnamicharts/airflow
```

## Deploying to OpenShift
```sh
helm install test-release-af .
```

## Visit the application here:
http://nr-airflow-dev.apps.emerald.devops.gov.bc.ca/

## Upgrade OpenShift Deployment
```sh 
helm upgrade -f values.yaml test-release-af . --version 16.1.2  
```

## Create OpenShift ConfigMap for requirements.txt: 
```sh
oc create configmap airflow-requirements --from-file=requirements.txt
```
Delete if already exists


More info: https://apps.nrs.gov.bc.ca/int/confluence/x/zQ09Cg

