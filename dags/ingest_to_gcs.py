import os
import logging

from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


workflow = DAG(
    start_date=datetime(2021, 2, 1),
    dag_id="IngestionDAG",
    schedule_interval="0 7 2 * *"
)
url = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv'
with workflow:
    wget_task = BashOperator(
        task_id="DownloadURL",
        bash_command=f'curl -sSL {url} > {AIRFLOW_HOME}/output.csv'
    )

    wget_task_2 = BashOperator(
        task_id="wget_task_2",
        bash_command=f'ls {AIRFLOW_HOME}'
    )

    wget_task >> wget_task_2