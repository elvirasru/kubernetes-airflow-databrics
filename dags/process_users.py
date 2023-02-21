from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import json
import csv
from datetime import datetime


USERS_END_POINT = 'api/v2/users?size=20'


def extract_user_information(task_instance):
    users = task_instance.xcom_pull(task_ids="extract_users")
    processed_users = []
    for user in users:
        processed_user = {
            'firstname': user['first_name'],
            'lastname': user['last_name'],
            'country': user['address']['country'],
            'gender': user['gender'],
            'employment': user['employment']['title'],
            'email': user['email']}
        processed_users.append(processed_user)
    return processed_users


def create_csv(task_instance):
    mylist = task_instance.xcom_pull(task_ids="process_user")
    with open('/tmp/processed_user.csv', 'w', newline='') as myfile:
        wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
        wr.writerow(mylist)


def store_user():
    hook = PostgresHook(postgres_conn_id='postgres')
    hook.copy_expert(
        sql="COPY users FROM stdin WITH DELIMITER as ','",
        filename='/tmp/processed_user.csv'
    )


with DAG('process_users', start_date=datetime(2022, 2, 1), schedule='@daily', catchup=False) as dag:
    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='users_api',
        endpoint=USERS_END_POINT
    )

    get_users = SimpleHttpOperator(
        task_id='get_users',
        http_conn_id='user_api',
        endpoint=USERS_END_POINT,
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    extract_user_information = PythonOperator(
        task_id='extract_user_information',
        python_callable=extract_user_information
    )

    create_csv_file = PythonOperator(
        task_id='create_csv_file',
        python_callable=create_csv
    )

    store_users = PythonOperator(
        task_id='store_users',
        python_callable=store_user
    )

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS users (
                firstname TEXT NOT NULL,
                lastname TEXT NOT NULL,
                country TEXT NOT NULL,
                gender TEXT NOT NULL,
                employment TEXT NOT NULL,
                email TEXT NOT NULL
            );
            '''
    )

    is_api_available >> get_users >> extract_user_information >> create_csv_file >> create_table >> store_users