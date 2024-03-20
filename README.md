# Airflow Set Up for NRM Data Analytics Platform

## Helm chart sourced from
```sh
helm pull oci://registry-1.docker.io/bitnamicharts/airflow
```

## Deploying to OpenShift
Navigate to the 'oc' folder then:
```sh
oc apply -f .
```
Navigate to the 'airflow' folder then:
```sh
helm install airflow .
```

## Visit the application here:
http://nr-airflow.apps.emerald.devops.gov.bc.ca/

## Upgrade OpenShift Deployment
```sh 
helm upgrade -f values.yaml airflow .
```

More info: https://apps.nrs.gov.bc.ca/int/confluence/x/zQ09Cg
