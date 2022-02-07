import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task, dag
from airflow.sensors.external_task import ExternalTaskSensor
from typing import Dict
from datetime import datetime

envs = {
    "env1": 'full_name_env1',
    "env2": 'full_name_env2',
    "env3": 'full_name_env3'
}

some_tables = {
    "table1": {
        "db_name": "db1",
        "table_name": "table1"
    },
    "table2": {
        "db_name": "db2",
        "table_name": "table2"
    }
}


@dag(dag_id='dag-test-snapshots',
     description='some dag parent',
     start_date=datetime(2022, 1, 1),
     schedule_interval='1 1 * * 6',
     catchup=True,
     max_active_runs=1
     )
def partner_dag_parent():
    @task(task_id="get_data")
    def get_data():
        logging.info("Now running get_data")

    data = get_data()


dag_parent = partner_dag_parent()
