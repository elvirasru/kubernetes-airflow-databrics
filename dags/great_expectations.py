from airflow import DAG
from airflow.operators.python import PythonOperator

from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator
from datetime import datetime

# work in progress
with DAG('process_users_great_expectations', start_date=datetime(2022, 2, 1), schedule='@daily', catchup=False) as dag:

    ready = PythonOperator(
        task_id='ready',
        python_callable=lambda: print("I am ready!")
    )

    ge_data_context_root_dir_with_checkpoint_name_pass = GreatExpectationsOperator(
        task_id="ge_data_context_root_dir_with_checkpoint_name_pass",
        data_context_root_dir='expectations',
        checkpoint_name="a.pass.chk",
    )

    ready >> ge_data_context_root_dir_with_checkpoint_name_pass
