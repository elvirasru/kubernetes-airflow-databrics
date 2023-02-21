from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksRunNowOperator
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

MY_FILE = Dataset("/tmp/processed_users.csv")

with DAG(
        dag_id='my_first_databricks_operator',
        schedule=[MY_FILE],
        start_date=days_ago(1),
        catchup=False
) as dag:
    ready = PythonOperator(
        task_id='ready',
        python_callable=lambda: print("I am ready!")
    )

    first_databricks_job = DatabricksRunNowOperator(
        task_id='databrics_execute_job',
        databricks_conn_id="databricks",
        job_id=14)

    ready >> first_databricks_job
