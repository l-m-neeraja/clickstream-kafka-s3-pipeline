import json
import os
import logging
import time
from datetime import datetime

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer

from transformations import clean_and_transform_event, batch_process_data

# ---------------- Logging ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s"
)

# ---------------- Kafka Config ----------------
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = "clickstream_events"
GROUP_ID = "clickstream-consumer-group"

# ---------------- S3 Config ----------------
S3_BUCKET = os.getenv("S3_BUCKET")
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL")

s3 = boto3.client(
    "s3",
    endpoint_url=AWS_ENDPOINT_URL,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_DEFAULT_REGION"),
)

# ---------------- Consumer ----------------
consumer = Consumer({
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
})

consumer.subscribe([TOPIC])

logging.info(json.dumps({"status": "consumer_started"}))

# ---------------- Batching ----------------
BATCH_SIZE = 50
buffer = []

# ---------------- Helper: Write batch to S3 ----------------
def write_batch_to_s3(df: pd.DataFrame):
    now = datetime.utcnow()

    s3_key = (
        f"clickstream/"
        f"year={now.year}/"
        f"month={now.month:02d}/"
        f"day={now.day:02d}/"
        f"hour={now.hour:02d}/"
        f"{int(time.time())}-batch.parquet"
    )

    table = pa.Table.from_pandas(df)
    local_path = "/tmp/batch.parquet"
    pq.write_table(table, local_path)

    s3.upload_file(local_path, S3_BUCKET, s3_key)

    return s3_key

# ---------------- Poll Loop ----------------
try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            logging.error(json.dumps({
                "event": "consumer_error",
                "error": str(msg.error())
            }))
            continue

        try:
            event = json.loads(msg.value())
        except Exception as e:
            logging.error(json.dumps({
                "event": "json_deserialization_failed",
                "error": str(e)
            }))
            continue

        transformed = clean_and_transform_event(event)

        if transformed is None:
            logging.error(json.dumps({
                "event": "invalid_message",
                "payload": event
            }))
            continue

        buffer.append(transformed)

        if len(buffer) >= BATCH_SIZE:
            df = batch_process_data(buffer)

            try:
                s3_key = write_batch_to_s3(df)
                consumer.commit()

                logging.info(json.dumps({
                    "event": "batch_written",
                    "s3_path": f"s3://{S3_BUCKET}/{s3_key}",
                    "batch_size": len(df)
                }, default=str))

                buffer.clear()

            except Exception as e:
                logging.error(json.dumps({
                    "event": "s3_write_failed",
                    "error": str(e)
                }))

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
