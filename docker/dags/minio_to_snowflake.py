"""
Docstring for docker.dags.minio_to_snowflake
Author - Abhay Mudgal
This script is used to fetch json data from minio and load it into bronze layer in snowflake
"""
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3
import json 
import snowflake.connector
from dotenv import load_dotenv

# ------------------------------------------------------
#               LOAD ENVIRONMENT VARIABLES
# ------------------------------------------------------
load_dotenv(dotenv_path="/opt/airflow/dags/.env")

# ----- MinIO Configuration -----
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
MINIO_PREFIX = os.getenv("MINIO_PREFIX")

# -------- Snowflake Configuration --------
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_TABLE = os.getenv("SNOWFLAKE_TABLE")

# ------ Local File Path ----------------------
LOCAL_TEMP_PATH = os.getenv("LOCAL_TEMP_PATH", "/tmp/spotify_raw.json")


def extract_from_minio():

    """
    Docstring for extract_from_minio
    Extract all .json event file from s3 and save locally
    """

    s3 = boto3.client(
        's3',
        endpoint_url = MINIO_ENDPOINT,
        aws_access_key_id = MINIO_ACCESS_KEY,
        aws_secret_access_key = MINIO_SECRET_KEY
    )

    response = s3.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=MINIO_PREFIX)
    contents = response.get("Contents", [])

    all_events = []
    for obj in contents:
        
        key = obj["Key"]
        
        # check if it is a json object
        if not key.endswith(".json"):
            continue
        
        # fetch data
        data = s3.get_object(Bucket=MINIO_BUCKET, Key=key)
        lines = data["Body"].read().decode('utf-8').splitlines()

        for line in lines:

            try:
                all_events.append(json.loads(line)) # json to dict

            except json.JSONDecodeError:
                continue
        
    # write in local temp path
    with open(LOCAL_TEMP_PATH, "w") as f:
        json.dump(all_events, f)
        
    print(f"Extracted {len(all_events)} events from MinIO and saved to {LOCAL_TEMP_PATH}")

    return LOCAL_TEMP_PATH

def load_to_snowflake(**context):

    """
    Docstring for load_to_snowflake
    Load raw data from local to snowflake bronze layer, no modifications
    """
    file_path = context["ti"].xcom_pull(task_ids="extract_data")

    with open(file_path, "r") as f:

        events = json.load(f)
    
    if not events:
        print("No events found to load")
        return 
    
    # create connection object to snowflake
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )

    # create table
    cur = conn.cursor()

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_TABLE}(
        EVENT_ID STRING,
        USER_ID STRING,
        SONG_ID STRING,
        ARTIST_NAME STRING,
        SONG_NAME STRING,
        EVENT_TYPE STRING,
        DEVICE_NAME STRING,
        COUNTRY STRING,
        TIMESTAMP STRING
    );
    """
    
    cur.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
    cur.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
    cur.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
    cur.execute(create_table_sql)
    
    # Load data in table
    insert_sql = f"""
    INSERT INTO {SNOWFLAKE_TABLE} (
        EVENT_ID, 
        USER_ID ,
        SONG_ID,
        ARTIST_NAME, 
        SONG_NAME,
        EVENT_TYPE, 
        DEVICE_NAME,
        COUNTRY ,
        TIMESTAMP
    ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    for event in events:
        cur.execute(insert_sql, (
            event.get("event_id"),
            event.get("user_id"),
            event.get("song_id"),
            event.get("artist_name"),
            event.get("song_name"),
            event.get("event_type"),
            event.get("device_name"),
            event.get("country"),
            event.get("timestamp")
        ))

    # Close connection 
    conn.commit()
    cur.close()
    conn.close()

    print(f"Loaded {len(events)} raw records into Snowflake table: {SNOWFLAKE_TABLE}")

    
    
default_args = {
    "owner" : "airflow",
    "start_date" : datetime(2026, 2, 17),
    "retries" : 1,
    "retry_delay" : timedelta(minutes=5)
}

with DAG(
    "spotify_minio_to_snowflake_bronze",
    default_args=default_args,
    schedule_interval="@hourly",
    catchup=False,
    description="Load raw Spotify events from MinIO to Snowflake Bronze table",
) as dag:
    
    # extract data from minio
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable = extract_from_minio
    )

    # load data to snowflake
    load_task = PythonOperator(
        task_id = 'load_data',
        python_callable = load_to_snowflake
    )
    
    # dependencies
    extract_task >> load_task