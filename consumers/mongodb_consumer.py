import os
import json
from datetime import datetime, timezone

from kafka import KafkaConsumer
from pymongo import MongoClient, InsertOne
from pymongo.errors import BulkWriteError, PyMongoError


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BROKER = "localhost:9092"
TOPIC = "supply_chain_events"

# Paste the SAME MongoDB Atlas connection string
# you used successfully before.
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")

DB_NAME = "supply_chain_db"

# IMPORTANT:
# Keep the SAME Atlas consumer group.
# Kafka will continue from the offsets already committed
# by the previous Atlas run.
MONGO_CONSUMER_GROUP = "supply-chain-mongodb-atlas-store"

# Number of Kafka events processed per batch.
BATCH_SIZE = 10


# ============================================================
# EVENT TYPE → COLLECTION
# ============================================================

EVENT_COLLECTION_MAP = {
    "ORDER_ITEM_CREATED": "orders",
    "INVENTORY_STATUS_RECORDED": "inventory",
    "RETAIL_SALE_RECORDED": "retail_sales",
    "MANUFACTURING_BATCH_RECORDED": "manufacturing",
    "SHIPMENT_STATUS_RECORDED": "logistics",
    "CUSTOMER_ACTIVITY_RECORDED": "customer_activity",
    "WEATHER_OBSERVED": "weather",
}


# ============================================================
# MONGODB ATLAS
# ============================================================

print("=" * 70)
print("Connecting to MongoDB Atlas...")
print("=" * 70)

mongo_client = MongoClient(
    MONGO_URI,
    serverSelectionTimeoutMS=10000
)

mongo_client.admin.command("ping")

db = mongo_client[DB_NAME]

print("MongoDB Atlas connection successful")
print(f"Database : {DB_NAME}")


# ============================================================
# KAFKA CONSUMER
# ============================================================

consumer = KafkaConsumer(
    TOPIC,

    bootstrap_servers=[
        KAFKA_BROKER
    ],

    auto_offset_reset="earliest",

    enable_auto_commit=False,

    group_id=MONGO_CONSUMER_GROUP,

    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    ),

    # Fetch more data efficiently during historical replay.
    max_poll_records=BATCH_SIZE,
)


# ============================================================
# STARTUP INFORMATION
# ============================================================

print("=" * 70)
print("MongoDB Atlas BATCH Consumer Started")
print(f"Kafka Broker   : {KAFKA_BROKER}")
print(f"Kafka Topic    : {TOPIC}")
print(f"Consumer Group : {MONGO_CONSUMER_GROUP}")
print(f"Atlas Database : {DB_NAME}")
print(f"Batch Size     : {BATCH_SIZE:,}")
print("=" * 70)


# ============================================================
# COUNTERS
# ============================================================

processed_count = 0
duplicate_count = 0
ignored_count = 0

batch = []


# ============================================================
# WRITE BATCH TO ATLAS
# ============================================================

def flush_batch():

    global batch
    global processed_count
    global duplicate_count

    if not batch:
        return

    # Separate the batch according to target collection.
    collection_batches = {}

    for collection_name, document in batch:

        if collection_name not in collection_batches:
            collection_batches[collection_name] = []

        collection_batches[collection_name].append(
            InsertOne(document)
        )


    # --------------------------------------------------------
    # BULK WRITE EACH COLLECTION
    # --------------------------------------------------------

    for collection_name, operations in collection_batches.items():

        try:

            result = db[collection_name].bulk_write(
                operations,
                ordered=False
            )

            processed_count += result.inserted_count


        except BulkWriteError as error:

            details = error.details

            inserted = details.get(
                "nInserted",
                0
            )

            processed_count += inserted


            # Count duplicate-key errors separately.
            write_errors = details.get(
                "writeErrors",
                []
            )

            non_duplicate_errors = []

            for write_error in write_errors:

                if write_error.get("code") == 11000:

                    duplicate_count += 1

                else:

                    non_duplicate_errors.append(
                        write_error
                    )


            # If Atlas reported an error other than
            # a duplicate key, stop processing.
            if non_duplicate_errors:

                print(
                    "\nMongoDB Atlas bulk write failed."
                )

                print(
                    non_duplicate_errors
                )

                raise


    # --------------------------------------------------------
    # COMMIT KAFKA OFFSETS
    # --------------------------------------------------------

    # All MongoDB writes in this batch have now either:
    #
    # 1. been inserted successfully
    # OR
    # 2. been confirmed as duplicate events.
    #
    # Therefore Kafka offsets can now be committed.

    consumer.commit()


    total_seen = (
        processed_count
        + duplicate_count
        + ignored_count
    )


    print(
        f"Processed: {processed_count:,} | "
        f"Duplicates: {duplicate_count:,} | "
        f"Ignored: {ignored_count:,} | "
        f"Total Seen: {total_seen:,}"
    )


    batch = []


# ============================================================
# CONSUME EVENTS
# ============================================================

try:

    for message in consumer:

        event = message.value

        event_type = event.get(
            "event_type"
        )


        collection_name = EVENT_COLLECTION_MAP.get(
            event_type
        )


        # ----------------------------------------------------
        # UNKNOWN EVENT
        # ----------------------------------------------------

        if collection_name is None:

            ignored_count += 1

            print(
                f"IGNORED unknown event type: "
                f"{event_type}"
            )

            continue


        # ----------------------------------------------------
        # PREPARE DOCUMENT
        # ----------------------------------------------------

        document = dict(event)


        document["ingested_at"] = datetime.now(
            timezone.utc
        )


        document["kafka_metadata"] = {
            "topic": message.topic,
            "partition": message.partition,
            "offset": message.offset,
        }


        batch.append(
            (
                collection_name,
                document
            )
        )


        # ----------------------------------------------------
        # FLUSH WHEN BATCH IS FULL
        # ----------------------------------------------------

        if len(batch) >= BATCH_SIZE:

            flush_batch()


# ============================================================
# MANUAL STOP
# ============================================================

except KeyboardInterrupt:

    print(
        "\nStop requested by user."
    )


    # Save any remaining records that have already
    # been read from Kafka before shutting down.

    if batch:

        print(
            f"Flushing final partial batch "
            f"({len(batch):,} events)..."
        )

        flush_batch()


# ============================================================
# DATABASE / KAFKA ERROR
# ============================================================

except PyMongoError as error:

    print(
        "\nMongoDB Atlas error."
    )

    print(error)

    raise


# ============================================================
# CLEAN SHUTDOWN
# ============================================================

finally:

    consumer.close()

    mongo_client.close()


    print()
    print("=" * 60)
    print("Final MongoDB Atlas Batch Consumer Summary")
    print("=" * 60)

    print(
        f"Inserted   : {processed_count:,}"
    )

    print(
        f"Duplicates : {duplicate_count:,}"
    )

    print(
        f"Ignored    : {ignored_count:,}"
    )

    print("=" * 60)