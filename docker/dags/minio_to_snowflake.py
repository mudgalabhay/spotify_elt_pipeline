"""
Docstring for docker.dags.minio_to_snowflake
Author - Abhay Mudgal
This script is used to fetch json data from minio and load it into bronze layer in snowflake
"""
from datetime import datetime, timedelta
from airflow import DAG



default_args = {
    "owner" : "airflow",
    "start_date" : datetime(2026, 2, 17),
    "retries" : 1,
    "retry_delay" : timedelta(minutes=5)
}

     