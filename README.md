# Airflow Set Up

## Helm chart sourced from
```sh
helm pull oci://registry-1.docker.io/bitnamicharts/airflow
```

## Deploying to OpenShift
```sh
helm install dev-release-af .
```

## Get the application running with these commands:
```sh
oc --namespace a1b9b0-dev port-forward svc/dev-release-af-airflow 8080:80
```

Hooking Airbyte: https://airbyte.com/blog/orchestrating-airbyte-api-airbyte-cloud-airflow

More info: https://apps.nrs.gov.bc.ca/int/confluence/x/zQ09Cg


