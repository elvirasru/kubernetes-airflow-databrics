# kubernetes-airflow-databrics

Requirements:
- kind
- kubectl
- helm
- podman


# 1. Create Kubernetes Cluster

```commandline
kind create cluster --name airflow-cluster --config kind-cluster.yaml
```

Check the nodes where properly created:
```commandline
kubectl get nodes
```

# 2. Create Kubernetes Namespace

```commandline
kubectl create namespace airflow
```

# 3. Add airflow

```commandline
helm install airflow apache-airflow/airflow --namespace airflow --debug
```

Now, airflow will be accessible in http://localhost:8080/ by executing the following command:
```commandline
kubectl port-forward svc/airflow-webserver 8080:8080 --namespace airflow
```

# 4. Add DAGs configuration

In order to run dags in aiflow, it is needed to define the location of them. There are several ways of configuring them. 
In this case, the following git repository will be defined as the place where retrieve the dags.

```commandline
helm upgrade --install airflow apache-airflow/airflow -n airflow \
  --set dags.gitSync.subPath="dags" \
  --set dags.gitSync.branch=main \
  --set dags.gitSync.repo=https://github.com/elvirasru/kubernetes-airflow-databrics.git \
  --set dags.gitSync.enabled=true
```

----------
How to delete the cluster:
```commandline
kind delete cluster --name=airflow-cluster 
```
