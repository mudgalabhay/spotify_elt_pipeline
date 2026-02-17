import os 
import json
import datetime
import boto3 
from kafka import KafkaConsumer
from dotenv import load_dotenv

# -------------------- Load environment variables -------------------------
load_dotenv()

# -------------------- Configuration --------------------------
# MinIO config
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

# Kafka config
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID")

BATCH_SIZE =  os.getenv("BATCH_SIZE")

# ------------- Connect to MinIO --------------------
s3 = boto3.client(
    "s3",
    endpoint_url = MINIO_ENDPOINT,
    aws_access_key_id = MINIO_ACCESS_KEY,
    aws_secret_access_key = MINIO_SECRET_KEY
)

# Ensure bucket exists if not create the same
try:    
    s3.head_bucket(Bucket=MINIO_BUCKET)
    
    print(f"Bucket {MINIO_BUCKET} already exists.")

except Exception:
    s3.create_bucket(Bucket=MINIO_BUCKET)
    print(f"Bucket {MINIO_BUCKET} created.")

# ------------- Kafka Consumer ----------------------
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    auto_offset_reset="earliest", # it will continue from where it last stopped
    enable_auto_commit=True,
    group_id=KAFKA_GROUP_ID, #  where the offsets the save, if we change this whole offset will start again
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print(f"Listening of events on Kafka topic '{KAFKA_TOPIC}'.........")

# store data from event in s3
try:

    def store_events(consumer, BATCH_SIZE):
        batch = []

        for message in consumer:
            event = message.value
            batch.append(event)

            if len(batch) >= int(BATCH_SIZE):
                now = datetime.datetime.utcnow()
                date_path = now.strftime("date=%Y-%m-%d/hour=%H")
                file_name = f"spotify_events_{now.strftime('%Y-%m-%dT%H-%M-%S')}.json"
                file_path = f"bronze/{date_path}/{file_name}"

                json_data = "\n".join([json.dumps(e) for e in batch])

                s3.put_object(
                    Bucket=MINIO_BUCKET,
                    Key=file_path,
                    Body=json_data.encode("utf-8")
                )

                print(f"Uploaded {len(batch)} events to MinIO: {file_path}")
                batch = []

    store_events(consumer, BATCH_SIZE)

except KeyboardInterrupt:
    print("Gracefully shutting down consumer...")