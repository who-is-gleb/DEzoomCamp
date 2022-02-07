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
from airflow.operators.python import get_current_context

envs = {
    "env1": 'full_name_env1',
    "env2": 'full_name_env2',
    "env3": 'full_name_env3',
    "env4": 'full_name_env4'
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

default_args = {
    "depends_on_past": False
}


@dag(
    dag_id='dag-test-dop_catchup_22222',
    description='some dag parent',
    start_date=datetime(2022, 2, 1),
    schedule_interval='0 2 * * *',
    catchup=True,
    default_args=default_args,
    max_active_runs=1
)
def partner_dag_parent():
    for env, env_full in envs.items():
        @task(task_id=f"get_data_{env}")
        def get_data():
            context = get_current_context()
            exec_date = context['execution_date']
            logging.info(f"Now running get_data on env {env}. Date {exec_date}.")

        data = get_data()


dag_parent = partner_dag_parent()
