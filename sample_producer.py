import json
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError

ROOT = Path(__file__).resolve().parent
SAMPLE_FILE = (
    ROOT / "data/assignment2_sample/supply_chain_sample.json"
)

KAFKA_BROKER = "localhost:9092"
TOPIC = "supply_chain_events"
STREAM_DELAY_SECONDS = 0.5


def main():
    with SAMPLE_FILE.open("r", encoding="utf-8") as file:
        events = json.load(file)

    if not isinstance(events, list) or not events:
        raise ValueError("Sample file must contain a non-empty JSON list.")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda value: json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8"),
        acks="all",
    )

    try:
        if not producer.partitions_for(TOPIC):
            raise RuntimeError(f"Topic not available: {TOPIC}")

        print(f"Connected to Kafka at {KAFKA_BROKER}")
        print(f"Streaming {len(events)} sample records to {TOPIC}\n")

        for number, event in enumerate(events, start=1):
            metadata = producer.send(
                TOPIC,
                value=event,
            ).get(timeout=30)

            print(
                f"Sent #{number} | "
                f"source={event['source_system']} | "
                f"entity={event['entity_id']} | "
                f"partition={metadata.partition} | "
                f"offset={metadata.offset}"
            )

            time.sleep(STREAM_DELAY_SECONDS)

        producer.flush()
        print(
            f"\nCompleted successfully. "
            f"{len(events)} messages acknowledged by Kafka."
        )

    finally:
        producer.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nProducer stopped.")
    except (KafkaError, OSError, ValueError, KeyError, RuntimeError) as error:
        print(f"Producer failed: {error}")
        raise SystemExit(1)