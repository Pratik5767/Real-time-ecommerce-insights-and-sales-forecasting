import json
import random
import time
import os
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer
from dotenv import load_dotenv

# Load credentials from .env file
load_dotenv()

fake = Faker()

# Azure Fabric Event Hub credentials loaded from .env
EVENT_HUB_NAMESPACE         = os.getenv("EVENT_HUB_NAMESPACE")
EVENT_HUB_CONNECTION_STRING = os.getenv("EVENT_HUB_CONNECTION_STRING")
EVENT_HUB_NAME              = os.getenv("EVENT_HUB_NAME")

producer = KafkaProducer(
    bootstrap_servers=EVENT_HUB_NAMESPACE,
    security_protocol='SASL_SSL',
    sasl_mechanism='PLAIN',
    sasl_plain_username='$ConnectionString',        # Must be literally '$ConnectionString'
    sasl_plain_password=EVENT_HUB_CONNECTION_STRING,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),

    # Fabric Eventstream stability settings
    request_timeout_ms=30000,       # Wait up to 30s for broker response
    retry_backoff_ms=500,           # Wait 500ms between retries
    retries=5,                      # Retry up to 5 times on transient failures
    acks='all',                     # Wait for all replicas to confirm (no data loss)
    linger_ms=100,                  # Batch messages for 100ms before sending (efficiency)
    batch_size=16384,               # Max batch size in bytes (16 KB)
    max_block_ms=60000,             # Wait up to 60s if buffer is full before raising error

    # compression_type='gzip',        # Enable at high throughput (100+ msg/s) to reduce Event Hub ingestion cost ~70
)

categories = ['Electronics', 'Books', 'Clothing', 'Home Decor', 'Toys']
locations = [
    {"city": "New York",    "state": "NY", "lat": 40.7128, "lon": -74.0060},
    {"city": "Los Angeles", "state": "CA", "lat": 34.0522, "lon": -118.2437},
    {"city": "Chicago",     "state": "IL", "lat": 41.8781, "lon": -87.6298},
    {"city": "Houston",     "state": "TX", "lat": 29.7604, "lon": -95.3698},
    {"city": "Phoenix",     "state": "AZ", "lat": 33.4484, "lon": -112.0740}
]

def generate_order():
    location = random.choice(locations)
    category = random.choice(categories)
    price    = round(random.uniform(10, 2000), 2)
    quantity = random.randint(1, 5)

    return {
        "order_id"          : fake.uuid4(),
        "timestamp"         : datetime.utcnow().isoformat() + "Z",  # Added 'Z' → explicit UTC marker
        "customer_id"       : fake.uuid4(),
        "product_id"        : fake.uuid4(),
        "category"          : category,
        "price"             : price,
        "quantity"          : quantity,
        "total_amount"      : round(price * quantity, 2),
        "city"              : location["city"],
        "state"             : location["state"],
        "country"           : "USA",
        "latitude"          : location["lat"],
        "longitude"         : location["lon"],
        "delivery_status"   : random.choice(["Processing", "Shipped", "Delivered", "Cancelled"])
    }

def on_success(metadata):
    print(f"✅ Sent to partition {metadata.partition} | offset {metadata.offset}")

def on_error(e):
    print(f"❌ Send failed: {e}")


if __name__ == "__main__":
    print("Streaming fake U.S. e-commerce orders to Microsoft Fabric Eventstream...")
    try:
        while True:
            event = generate_order()
            producer.send(EVENT_HUB_NAME, value=event) \
                    .add_callback(on_success) \
                    .add_errback(on_error)
            print("Queued:", event)
            time.sleep(2)
    except KeyboardInterrupt:
        print("\n⏹ Stopping producer...")
    finally:
        producer.flush()   # Push all buffered messages before exit
        producer.close()   # Clean connection teardown
        print("Producer closed cleanly.")