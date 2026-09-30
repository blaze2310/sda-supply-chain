import json
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError


KAFKA_BROKER = "localhost:9092"
TOPIC = "supply_chain_events"

ORDERS_FILE = Path(
    "data/cleaned/orders/orders_cleaned.xlsx"
)

CUSTOMER_MASTER_FILE = Path(
    "data/cleaned/customer/customer_master.xlsx"
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


print("Loading finalized customer and order data...")

orders = pd.read_excel(ORDERS_FILE)

customer_master = pd.read_excel(
    CUSTOMER_MASTER_FILE
)


required_order_columns = [
    "customer_id",
    "product_id",
    "product_name",
    "order_id",
    "order_item_id",
    "location_id",
]


missing_order_columns = [
    column
    for column in required_order_columns
    if column not in orders.columns
]


if missing_order_columns:
    raise ValueError(
        "Orders file is missing required columns: "
        + ", ".join(missing_order_columns)
    )


if "customer_id" not in customer_master.columns:
    raise ValueError(
        "Customer master is missing customer_id."
    )


valid_orders = orders[
    required_order_columns
].dropna(
    subset=[
        "customer_id",
        "product_id",
        "order_id",
    ]
)


customer_lookup = (
    customer_master
    .drop_duplicates("customer_id")
    .set_index("customer_id")
    .to_dict("index")
)


valid_orders = valid_orders[
    valid_orders["customer_id"].isin(
        customer_lookup.keys()
    )
].reset_index(drop=True)


if valid_orders.empty:
    raise ValueError(
        "No valid customer-order-product relationships found."
    )


print(
    f"Loaded {len(valid_orders)} valid "
    f"customer-product relationships."
)


sessions = {}


def get_session_id(customer_id):

    if customer_id not in sessions:
        sessions[customer_id] = (
            "SESSION-"
            + uuid.uuid4().hex[:12].upper()
        )

    return sessions[customer_id]


try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda value: json.dumps(
            value,
            default=str
        ).encode("utf-8"),
    )

    producer.partitions_for(TOPIC)

    print(
        f"Connected to Kafka at {KAFKA_BROKER}"
    )

except KafkaError as error:

    print(
        f"Kafka connection failed: {error}"
    )

    raise SystemExit(1)


OUTPUT_FILE.parent.mkdir(
    parents=True,
    exist_ok=True
)


event_number = 1


print(
    "Generating customer activity from "
    "valid customer-product relationships..."
)

print("Press Control + C to stop.\n")


try:

    with OUTPUT_FILE.open(
        "a",
        encoding="utf-8"
    ) as output_file:

        while True:

            relationship = valid_orders.sample(
                n=1
            ).iloc[0]

            customer_id = str(
                relationship["customer_id"]
            )

            product_id = str(
                relationship["product_id"]
            )

            customer = customer_lookup[
                customer_id
            ]

            session_id = get_session_id(
                customer_id
            )

            activity_type = random.choice(
                ACTIVITY_TYPES
            )

            if activity_type in {
                "add_to_cart",
                "checkout_started",
                "purchase",
            }:

                quantity = random.randint(
                    1,
                    5
                )

                cart_value = round(
                    random.uniform(
                        10,
                        500
                    ) * quantity,
                    2
                )

            else:

                quantity = 0
                cart_value = 0.0


            event_timestamp = (
                datetime.now(timezone.utc)
                .isoformat()
            )


            event_id = (
                "CUSTOMER-EVENT-"
                + uuid.uuid4().hex[:12].upper()
            )


            event = {

                "event_id": event_id,

                "event_type":
                    "CUSTOMER_ACTIVITY_RECORDED",

                "source_system":
                    "python_customer_simulator",

                "entity_id": session_id,

                "payload": {

                    "customer_activity_id":
                        f"ACTIVITY-{event_number:06d}",

                    "event_timestamp":
                        event_timestamp,

                    "customer_id":
                        customer_id,

                    "customer_name":
                        customer.get(
                            "customer_name"
                        ),

                    "segment":
                        customer.get(
                            "segment"
                        ),

                    "session_id":
                        session_id,

                    "product_id":
                        product_id,

                    "product_name":
                        relationship[
                            "product_name"
                        ],

                    "order_id":
                        relationship[
                            "order_id"
                        ],

                    "order_item_id":
                        relationship[
                            "order_item_id"
                        ],

                    "location_id":
                        relationship[
                            "location_id"
                        ],

                    "activity_type":
                        activity_type,

                    "device_type":
                        random.choice(
                            DEVICE_TYPES
                        ),

                    "traffic_source":
                        random.choice(
                            TRAFFIC_SOURCES
                        ),

                    "quantity":
                        quantity,

                    "cart_value":
                        cart_value,
                },
            }


            producer.send(
                TOPIC,
                value=event
            )


            output_file.write(
                json.dumps(
                    event,
                    default=str
                )
                + "\n"
            )

            output_file.flush()


            print(
                f"Sent {event_id} | "
                f"customer={customer_id} | "
                f"product={product_id} | "
                f"activity={activity_type}"
            )


            event_number += 1

            time.sleep(
                STREAM_DELAY_SECONDS
            )


except KeyboardInterrupt:

    print(
        "\nCustomer activity streaming stopped."
    )


finally:

    producer.flush()
    producer.close()