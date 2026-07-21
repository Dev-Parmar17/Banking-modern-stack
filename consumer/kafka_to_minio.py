import boto3
from kafka import KafkaConsumer
from kafka.serializer import Deserializer
import json
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv

# -----------------------------
# Load secrets from .env
# -----------------------------
load_dotenv()

# Defaults for local Docker setup
DEFAULT_KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:29092')
DEFAULT_KAFKA_GROUP = os.getenv('KAFKA_GROUP', 'banking-consumer-group')
DEFAULT_MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
DEFAULT_MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', os.getenv('MINIO_ROOT_USER'))
DEFAULT_MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', os.getenv('MINIO_ROOT_PASSWORD'))
DEFAULT_MINIO_BUCKET = os.getenv('MINIO_BUCKET', 'banking-data')

KAFKA_TOPICS = [
    'banking.public.customers',
    'banking.public.accounts',
    'banking.public.transactions'
]

print(f"Kafka bootstrap: {DEFAULT_KAFKA_BOOTSTRAP}")
print(f"Kafka group: {DEFAULT_KAFKA_GROUP}")
print(f"Kafka topics: {KAFKA_TOPICS}")
print(f"MinIO endpoint: {DEFAULT_MINIO_ENDPOINT}")
print(f"MinIO bucket: {DEFAULT_MINIO_BUCKET}")


class JsonDeserializer(Deserializer):
    def __call__(self, value):
        if value is None:
            return None
        if isinstance(value, (bytes, bytearray)):
            value = value.decode('utf-8')
        return json.loads(value)

    def deserialize(self, topic, headers, value):
        return self.__call__(value)


# Kafka consumer settings
consumer = KafkaConsumer(
    *KAFKA_TOPICS,
    bootstrap_servers=DEFAULT_KAFKA_BOOTSTRAP,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=DEFAULT_KAFKA_GROUP,
    value_deserializer=JsonDeserializer()
)

# MinIO client
s3 = boto3.client(
    's3',
    endpoint_url=DEFAULT_MINIO_ENDPOINT,
    aws_access_key_id=DEFAULT_MINIO_ACCESS_KEY,
    aws_secret_access_key=DEFAULT_MINIO_SECRET_KEY
)

bucket = DEFAULT_MINIO_BUCKET

# Create bucket if not exists
existing_buckets = [b['Name'] for b in s3.list_buckets()['Buckets']]
if bucket not in existing_buckets:
    s3.create_bucket(Bucket=bucket)

# Consume and write function
def write_to_minio(table_name, records):
    if not records:
        return
    df = pd.DataFrame(records)
    date_str = datetime.now().strftime('%Y-%m-%d')
    file_path = f'{table_name}_{date_str}.parquet'
    # Prefer pyarrow (more robust). If it's unavailable or fails,
    # convert string/object columns to plain Python str and try fastparquet.
    try:
        df.to_parquet(file_path, engine='pyarrow', index=False)
    except Exception:
        try:
            for c in df.select_dtypes(include=['object', 'string']):
                df[c] = df[c].astype(str)
            df.to_parquet(file_path, engine='fastparquet', index=False)
        except Exception as e:
            print(f"❌ Failed to write parquet file: {e}")
            return
    s3_key = f'{table_name}/date={date_str}/{table_name}_{datetime.now().strftime("%H%M%S%f")}.parquet'
    s3.upload_file(file_path, bucket, s3_key)
    os.remove(file_path)
    print(f'✅ Uploaded {len(records)} records to s3://{bucket}/{s3_key}')

# Batch consume
batch_size = 50
buffer = {topic: [] for topic in KAFKA_TOPICS}

print("✅ Connected to Kafka. Listening for messages...")

for message in consumer:
    topic = message.topic
    event = message.value
    payload = event.get("payload", {})
    record = payload.get("after")  # Only take the actual row

    if record:
        buffer[topic].append(record)
        print(f"[{topic}] -> {record}")  # Debugging

    if len(buffer[topic]) >= batch_size:
        write_to_minio(topic.split('.')[-1], buffer[topic])
        buffer[topic] = []