import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_FILE = (
    ROOT / "data/assignment2_sample/supply_chain_sample.json"
)

# File, source system, event type, entity field, ID prefix
SOURCES = [
    (
        "data/cleaned/orders/global_superstore_enriched.xlsx",
        "global_superstore",
        "ORDER_ITEM_CREATED",
        "order_item_id",
        "GSITEM",
    ),
    (
        "data/cleaned/manufacturing/manufacturing_enriched.xlsx",
        "smart_manufacturing",
        "MANUFACTURING_BATCH_RECORDED",
        "manufacturing_record_id",
        "MFGREC",
    ),
    (
        "data/cleaned/logistics/ecommerce_shipping_enriched.xlsx",
        "ecommerce_shipping",
        "SHIPMENT_STATUS_RECORDED",
        "shipment_id",
        "SHIP",
    ),
    (
        "data/cleaned/inventory/retail_inventory_enriched.csv",
        "retail_inventory",
        "INVENTORY_STATUS_RECORDED",
        "inventory_record_id",
        "INV",
    ),
    (
        "data/cleaned/inventory/retail_sales_enriched.csv",
        "retail_sales",
        "RETAIL_SALE_RECORDED",
        "sales_record_id",
        "SALE",
    ),
]


def clean_value(value):
    if pd.isna(value):
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if hasattr(value, "item"):
        return value.item()
    return value


events = []

for filename, source, event_type, entity_field, prefix in SOURCES:
    path = ROOT / filename

    if path.suffix == ".csv":
        data = pd.read_csv(path, nrows=25)
    else:
        data = pd.read_excel(path, nrows=25)

    for number, (_, row) in enumerate(data.iterrows(), start=1):
        payload = {
            column: clean_value(value)
            for column, value in row.items()
        }

        entity_id = payload.get(entity_field)
        if not entity_id:
            raise ValueError(
                f"Missing {entity_field} in {filename}, record {number}"
            )

        events.append({
            "event_id": f"MAIN-{prefix}-EVENT-{number:06d}",
            "event_type": event_type,
            "source_system": source,
            "entity_id": entity_id,
            "payload": payload,
        })

    print(f"{source}: {len(data)} records prepared")

OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

with OUTPUT_FILE.open("w", encoding="utf-8") as file:
    json.dump(
        events,
        file,
        indent=2,
        ensure_ascii=False,
        allow_nan=False,
    )

print(f"\nSaved {len(events)} sample records to:")
print(OUTPUT_FILE)