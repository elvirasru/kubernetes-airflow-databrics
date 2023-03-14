FROM apache/airflow:2.5.1-python3.9

#Databrics provider
RUN pip install apache-airflow-providers-databricks==4.0.0

#Great Expectations provider
COPY great-expectations/users-expectations.json /opt/airflow/expectations_suite/

RUN pip install airflow-provider-great-expectations==0.2.5