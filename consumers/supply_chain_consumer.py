import json
from collections import defaultdict

from kafka import KafkaConsumer
from kafka.errors import KafkaError

KAFKA_BROKER = "localhost:9092"
TOPIC = "supply_chain_events"

try:
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="supply-chain-assignment2",
        value_deserializer=lambda message: json.loads(
            message.decode("utf-8")
        ),
    )

except KafkaError as error:
    print(f"Kafka connection failed: {error}")
    raise SystemExit(1)


source_counts = defaultdict(int)
total_messages = 0

print(f"Listening to {TOPIC}...")
print("Press Control + C to stop.\n")

try:
    for message in consumer:
        event = message.value

        source = event.get("source_system", "unknown")
        event_type = event.get("event_type", "unknown")
        entity_id = event.get("entity_id", "unknown")

        source_counts[source] += 1
        total_messages += 1

        print(
            f"Received #{total_messages} | "
            f"source={source} | "
            f"type={event_type} | "
            f"entity={entity_id}"
        )

        print(json.dumps(event, indent=2, default=str))
        print("-" * 80)

        if total_messages % 5 == 0:
            print(f"Message counts: {dict(source_counts)}\n")

except KeyboardInterrupt:
    print("\nConsumer stopped.")

finally:
    consumer.close()