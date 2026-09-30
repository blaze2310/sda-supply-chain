import json
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError


KAFKA_BROKER = "localhost:9092"
TOPIC = "supply_chain_events"

FILE_PATH = Path(
    "data/cleaned/inventory/retail_sales_cleaned.xlsx"
)


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
            value,
            default=str
        ).encode("utf-8"),
    )

    producer.partitions_for(TOPIC)
    print(f"Connected to Kafka at {KAFKA_BROKER}")

except KafkaError as error:
    print(f"Kafka connection failed: {error}")
    raise SystemExit(1)


print(f"Loading retail sales from {FILE_PATH}...")

sales_data = pd.read_excel(FILE_PATH)

total_records = len(sales_data)

print(f"Loaded {total_records:,} retail sales records.")
print(f"Streaming all records to {TOPIC}...\n")


for record_number, (_, row) in enumerate(
    sales_data.iterrows(),
    start=1
):
    payload = {
        column: clean_value(value)
        for column, value in row.to_dict().items()
    }

    entity_id = (
        payload.get("sales_record_id")
        or f"SALE-{record_number:06d}"
    )

    event = {
        "event_id": f"SALE-EVENT-{record_number:06d}",
        "event_type": "RETAIL_SALE_RECORDED",
        "source_system": "retail_sales",
        "entity_id": entity_id,
        "payload": payload,
    }

    producer.send(
        TOPIC,
        key=str(entity_id).encode("utf-8"),
        value=event,
    )

    if (
        record_number == 1
        or record_number % 5000 == 0
        or record_number == total_records
    ):
        print(
            f"Retail Sales: {record_number:,}/"
            f"{total_records:,} sent"
        )


producer.flush()
producer.close()

print(
    f"\nRetail Sales completed: "
    f"{total_records:,} events sent."
)