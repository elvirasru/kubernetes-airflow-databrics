from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksRunNowOperator
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

my_file = Dataset("/tmp/processed_user.csv")

with DAG(
        dag_id='my_first_databricks_operator',
        schedule=[my_file],
        start_date=days_ago(1),
) as dag:

    ready = PythonOperator(
        task_id='ready',
        python_callable=lambda x: print("I am ready!")
    )

    first_databricks_job = DatabricksRunNowOperator(
        task_id='databrics_execute_job',
        databricks_conn_id="databricks",
        job_id=14)

    ready >> first_databricks_job
