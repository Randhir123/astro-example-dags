from airflow import DAG
from airflow.models.connection import Connection
from time import time_ns
from datetime import datetime
import os
import pendulum
import logging

from airflow.decorators import dag, task

conn = Connection(
    conn_id="aws_demo",
    conn_type="aws",
    extra={
        "config_kwargs": {
            "signature_version": "unsigned",
        },
    },
)

env_key = f"AIRFLOW_CONN_{conn.conn_id.upper()}"
conn_uri = conn.get_uri()
os.environ[env_key] = conn_uri

@dag(
    # schedule to run daily
    # once it is enabled in Airflow
    schedule_interval='@daily',
    start_date=pendulum.now()
)
def greet_flow():

    @task
    def hello_world():
        logging.info("Hello World!")

    # hello_world represents the invocation of the only task in this DAG
    # it will run by itself, without any sequence before or after another task
    hello_world_task=hello_world()

greet_flow_dag=greet_flow()
