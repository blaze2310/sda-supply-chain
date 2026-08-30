import json
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError

KAFKA_BROKER = "localhost:9092"
TOPIC = "supply_chain_events"
RECORDS_PER_SOURCE = 10
STREAM_DELAY_SECONDS = 0.5

SOURCES = [
    {
        "name": "orders",
        "path": Path(
            "data/cleaned/orders/global_superstore_enriched.xlsx"
        ),
        "source_system": "global_superstore",
        "event_type": "ORDER_ITEM_CREATED",
        "entity_field": "order_item_id",
        "fallback_prefix": "GSITEM",
    },
    {
        "name": "manufacturing",
        "path": Path(
            "data/cleaned/manufacturing/manufacturing_enriched.xlsx"
        ),
        "source_system": "smart_manufacturing",
        "event_type": "MANUFACTURING_BATCH_RECORDED",
        "entity_field": "manufacturing_record_id",
        "fallback_prefix": "MFGREC",
    },
    {
        "name": "logistics",
        "path": Path(
            "data/cleaned/logistics/ecommerce_shipping_enriched.xlsx"
        ),
        "source_system": "ecommerce_shipping",
        "event_type": "SHIPMENT_STATUS_RECORDED",
        "entity_field": "shipment_id",
        "fallback_prefix": "SHIP",
    },
    {
        "name": "inventory",
        "path": Path(
            "data/cleaned/inventory/retail_inventory_enriched.csv"
        ),
        "source_system": "retail_inventory",
        "event_type": "INVENTORY_STATUS_RECORDED",
        "entity_field": "inventory_record_id",
        "fallback_prefix": "INV",
    },
    {
        "name": "retail_sales",
        "path": Path(
            "data/cleaned/inventory/retail_sales_enriched.csv"
        ),
        "source_system": "retail_sales",
        "event_type": "RETAIL_SALE_RECORDED",
        "entity_field": "sales_record_id",
        "fallback_prefix": "SALE",
    },
]


def clean_value(value):
    if pd.isna(value):
        return None

    if hasattr(value, "isoformat"):
        return value.isoformat()

    if hasattr(value, "item"):
        return value.item()

    return value


def read_source(file_path):
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    if file_path.suffix.lower() == ".csv":
        return pd.read_csv(file_path)

    if file_path.suffix.lower() == ".xlsx":
        return pd.read_excel(file_path)

    raise ValueError(f"Unsupported file format: {file_path.suffix}")


try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda value: json.dumps(
            value,
            default=str,
        ).encode("utf-8"),
    )

    producer.partitions_for(TOPIC)
    print(f"Connected to Kafka at {KAFKA_BROKER}")
    print(f"Topic: {TOPIC}\n")

except KafkaError as error:
    print(f"Kafka connection failed: {error}")
    raise SystemExit(1)


total_sent = 0

try:
    for source in SOURCES:
        dataframe = read_source(source["path"]).head(
            RECORDS_PER_SOURCE
        )

        print(
            f"Streaming {len(dataframe)} records "
            f"from {source['name']}..."
        )

        for row_number, (_, row) in enumerate(
            dataframe.iterrows(),
            start=1,
        ):
            payload = {
                column: clean_value(value)
                for column, value in row.to_dict().items()
            }

            entity_id = payload.get(source["entity_field"])

            if not entity_id:
                entity_id = (
                    f"{source['fallback_prefix']}-"
                    f"{row_number:06d}"
                )

            event = {
                "event_id": (
                    f"MAIN-{source['fallback_prefix']}-EVENT-"
                    f"{row_number:06d}"
                ),
                "event_type": source["event_type"],
                "source_system": source["source_system"],
                "entity_id": entity_id,
                "payload": payload,
            }

            producer.send(TOPIC, value=event).get(timeout=10)

            total_sent += 1

            print(
                f"Sent #{total_sent} | "
                f"source={source['source_system']} | "
                f"type={source['event_type']} | "
                f"entity={entity_id}"
            )

            time.sleep(STREAM_DELAY_SECONDS)

        print()

    producer.flush()

    print(
        f"Completed successfully. "
        f"{total_sent} messages sent to {TOPIC}."
    )

except (KafkaError, FileNotFoundError, ValueError) as error:
    print(f"Producer failed: {error}")
    raise SystemExit(1)

finally:
    producer.close()