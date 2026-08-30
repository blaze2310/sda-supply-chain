import json
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError

KAFKA_BROKER = "localhost:9092"
TOPIC = "supply_chain_events"
FILE_PATH = Path(
    "data/cleaned/manufacturing/manufacturing_enriched.xlsx"
)
SAMPLE_SIZE = 30
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


manufacturing_data = pd.read_excel(FILE_PATH).head(SAMPLE_SIZE)

print(
    f"Streaming {len(manufacturing_data)} manufacturing records "
    f"to {TOPIC}...\n"
)

for index, row in manufacturing_data.iterrows():
    payload = {
        column: clean_value(value)
        for column, value in row.to_dict().items()
    }

    entity_id = (
        payload.get("manufacturing_record_id")
        or f"MFGREC-{index + 1:06d}"
    )

    event = {
        "event_id": f"MFG-EVENT-{index + 1:06d}",
        "event_type": "MANUFACTURING_BATCH_RECORDED",
        "source_system": "smart_manufacturing",
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

print("\nManufacturing streaming completed.")