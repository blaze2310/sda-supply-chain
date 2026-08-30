import json
import random
import time
import uuid
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError

KAFKA_BROKER = "localhost:9092"
TOPIC = "supply_chain_events"

ORDERS_FILE = Path(
    "data/cleaned/orders/global_superstore_enriched.xlsx"
)

OUTPUT_FILE = Path(
    "data/generated/customer_activity/customer_activity_events.jsonl"
)

STREAM_DELAY_SECONDS = 1

ACTIVITY_TYPES = [
    "product_view",
    "product_view",
    "product_view",
    "search",
    "add_to_cart",
    "remove_from_cart",
    "checkout_started",
    "purchase",
]

DEVICE_TYPES = [
    "mobile",
    "desktop",
    "tablet",
]

TRAFFIC_SOURCES = [
    "organic_search",
    "paid_ad",
    "social_media",
    "email",
    "direct",
]


orders = pd.read_excel(ORDERS_FILE)

customer_ids = (
    orders["customer_id"]
    .dropna()
    .astype(str)
    .unique()
    .tolist()
)

product_ids = (
    orders["product_id"]
    .dropna()
    .astype(str)
    .unique()
    .tolist()
)

if not customer_ids or not product_ids:
    print("No customer_id or product_id values were found.")
    raise SystemExit(1)


sessions = [
    {
        "session_id": f"SESSION-{index:06d}",
        "customer_id": random.choice(customer_ids),
    }
    for index in range(1, 21)
]


try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda value: json.dumps(
            value, default=str
        ).encode("utf-8"),
    )

    producer.partitions_for(TOPIC)
    print(f"Connected to Kafka at {KAFKA_BROKER}")

except KafkaError as error:
    print(f"Kafka connection failed: {error}")
    raise SystemExit(1)


OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

event_number = 1

print("Generating and streaming customer activity...")
print("Press Control + C to stop.\n")

try:
    with OUTPUT_FILE.open("a", encoding="utf-8") as output_file:
        while True:
            session = random.choice(sessions)
            activity_type = random.choice(ACTIVITY_TYPES)
            product_id = random.choice(product_ids)

            if activity_type in {
                "add_to_cart",
                "checkout_started",
                "purchase",
            }:
                quantity = random.randint(1, 5)
                cart_value = round(
                    random.uniform(10, 500) * quantity,
                    2,
                )
            else:
                quantity = 0
                cart_value = 0.0

            event_id = (
                "CUSTOMER-EVENT-"
                + uuid.uuid4().hex[:12].upper()
            )

            event = {
                "event_id": event_id,
                "event_type": "CUSTOMER_ACTIVITY_RECORDED",
                "source_system": "python_customer_simulator",
                "entity_id": session["session_id"],
                "payload": {
                    "customer_activity_id": (
                        f"ACTIVITY-{event_number:06d}"
                    ),
                    "customer_id": session["customer_id"],
                    "session_id": session["session_id"],
                    "product_id": product_id,
                    "activity_type": activity_type,
                    "device_type": random.choice(DEVICE_TYPES),
                    "traffic_source": random.choice(
                        TRAFFIC_SOURCES
                    ),
                    "quantity": quantity,
                    "cart_value": cart_value,
                },
            }

            producer.send(TOPIC, value=event)

            output_file.write(json.dumps(event) + "\n")
            output_file.flush()

            print(
                f"Sent {event_id} | "
                f"customer={session['customer_id']} | "
                f"product={product_id} | "
                f"activity={activity_type}"
            )

            event_number += 1
            time.sleep(STREAM_DELAY_SECONDS)

except KeyboardInterrupt:
    print("\nCustomer activity streaming stopped.")

finally:
    producer.flush()
    producer.close()