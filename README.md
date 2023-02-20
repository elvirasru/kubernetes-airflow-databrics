# kubernetes-airflow-databrics
(work in progress)

Requirements:
- kind
- kubectl
- helm
- podman

Follow the next steps to run airflow in a kubernetes cluster in your local environment:

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
helm repo add apache-airflow https://airflow.apache.org
```

```commandline
helm install airflow apache-airflow/airflow --namespace airflow --debug
```

Now, airflow will be accessible in http://localhost:8080/ by executing the following command:
```commandline
kubectl port-forward svc/airflow-webserver 8080:8080 --namespace airflow
```
Username: admin
Password: admin

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

# 5. Add Databrics provider

First, let's create a new airflow image ([Dockerfile](Dockerfile)) with the databrics provider (``apache-airflow-providers-databricks``)

```commandline
podman build --tag custom-airflow:0.0.1 .
podman save custom-airflow:0.0.1 -o custom-airflow.tar
```

Next, the image needs to be loaded into the cluster:
```commandline
kind load image-archive custom-airflow.tar --name airflow-cluster
```

To check whether the new image is in the cluster execute the following commands:
```commandline
kubectl get nodes
podman exec -ti airflow-cluster-worker bash
```
And inside the node execute ``crictl images``.


Finally:

```commandline
helm upgrade airflow apache-airflow/airflow -n airflow \
  --set dags.gitSync.subPath="dags" \
  --set dags.gitSync.branch=main \
  --set dags.gitSync.repo=https://github.com/elvirasru/kubernetes-airflow-databrics.git \
  --set dags.gitSync.enabled=true \
  --set images.airflow.repository=localhost/custom-airflow \
  --set images.airflow.tag=0.0.1
```

----------
How to delete the cluster:
```commandline
kind delete cluster --name=airflow-cluster 
```
