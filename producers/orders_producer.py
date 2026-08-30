import json
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError

KAFKA_BROKER = "localhost:9092"
TOPIC = "supply_chain_events"
FILE_PATH = Path("data/cleaned/orders/global_superstore_enriched.xlsx")
SAMPLE_SIZE = 35
STREAM_DELAY_SECONDS = 0.5


def clean_value(value):
    if pd.isna(value):
        return None

    if hasattr(value, "isoformat"):
        return value.isoformat()

    if hasattr(value, "item"):
        return value.item()

    return value


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


orders = pd.read_excel(FILE_PATH).head(SAMPLE_SIZE)

print(f"Streaming {len(orders)} order records to {TOPIC}...\n")

for index, row in orders.iterrows():
    payload = {
        column: clean_value(value)
        for column, value in row.to_dict().items()
    }

    entity_id = payload.get("order_item_id") or f"GSITEM-{index + 1:06d}"

    event = {
        "event_id": f"ORDER-EVENT-{index + 1:06d}",
        "event_type": "ORDER_ITEM_CREATED",
        "source_system": "global_superstore",
        "entity_id": entity_id,
        "payload": payload,
    }

    producer.send(TOPIC, value=event)

    print(
        f"Sent {event['event_id']} | "
        f"entity={entity_id} | "
        f"source={event['source_system']}"
    )

    time.sleep(STREAM_DELAY_SECONDS)


producer.flush()
producer.close()

print("\nOrder streaming completed.")