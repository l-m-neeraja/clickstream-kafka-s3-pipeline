import json
import os
import time
import logging
from datetime import datetime
from uuid import uuid4

from faker import Faker
from confluent_kafka import Producer

# ---------------- Logging ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s"
)

fake = Faker()

# ---------------- Kafka Config ----------------
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = "clickstream_events"

producer = Producer({
    "bootstrap.servers": KAFKA_BROKER
})

# ---------------- Event Generator ----------------
def generate_clickstream_event():
    return {
        "user_id": str(uuid4()),
        "event_type": fake.random_element([
            "view_product",
            "add_to_cart",
            "checkout"
        ]),
        "timestamp": datetime.utcnow().isoformat(),
        "page_url": fake.url(),
        "product_id": fake.random_int(min=1, max=1000),
        "session_id": str(uuid4()),
        "ip_address": fake.ipv4()
    }

# ---------------- Delivery Callback ----------------
def delivery_report(err, msg):
    if err:
        logging.error(json.dumps({
            "event": "delivery_failed",
            "error": str(err)
        }))

# ---------------- Main Loop ----------------
if __name__ == "__main__":
    logging.info(json.dumps({"status": "producer_started"}))

    while True:
        event = generate_clickstream_event()

        producer.produce(
            topic=TOPIC,
            value=json.dumps(event),
            callback=delivery_report
        )

        producer.poll(0)
        time.sleep(0.2)  # ~5 events/sec
