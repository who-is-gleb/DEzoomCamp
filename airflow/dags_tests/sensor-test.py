import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task, dag
from airflow.sensors.external_task import ExternalTaskSensor
from typing import Dict
from datetime import datetime, timedelta

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


@dag(dag_id='dag-test-moat',
     description='some dag',
     start_date=datetime(2022, 1, 1),
     schedule_interval='1 2 * * 6',
     catchup=True,
     max_active_runs=1
     )
def partner_dag():
    sensor_task = ExternalTaskSensor(
        task_id="child_task1",
        external_dag_id='dag-test-snapshots',
        timeout=300,
        execution_delta=timedelta(hours=1),
        allowed_states=['success'],
        failed_states=['failed'],
        mode="poke",
        poke_interval=10,
    )

    @task(task_id="get_data")
    def get_data():
        logging.info("Now running get_data")

    data = get_data()
    sensor_task >> data

    for table in some_tables:
        @task(task_id=f"send-trigger-{table}")
        def send_table_trigger():
            logging.info(f"Sending trigger for table {table}")

        trigger = send_table_trigger()

        for env, full_env in envs.items():
            @task(task_id=f"create-table-{table}-{env}")
            def create_table():
                logging.info(f"Creating table {table} on env {env}, full env {full_env}")

            created_table = create_table()
            data >> created_table
            created_table >> trigger


dag = partner_dag()
