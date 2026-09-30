import json

import mysql.connector
from kafka import KafkaConsumer


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BROKER = "localhost:9092"
TOPIC = "supply_chain_events"

# IMPORTANT:
# New group so this weather-only consumer does not interfere
# with the previous finalized MySQL consumer offsets.
CONSUMER_GROUP = "supply-chain-mysql-weather-historical"

MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "root",
    "database": "supply_chain_db",
}

TARGET_EVENT_TYPE = "WEATHER_HISTORICAL"


# ============================================================
# MYSQL CONNECTION
# ============================================================

try:
    connection = mysql.connector.connect(
        **MYSQL_CONFIG
    )

    cursor = connection.cursor()

    print("MySQL connection successful")

except mysql.connector.Error as error:

    print(
        f"MySQL connection failed: {error}"
    )

    raise SystemExit(1)


# ============================================================
# INSERT / UPSERT QUERY
# ============================================================

INSERT_QUERY = """
INSERT INTO weather (
    event_id,
    entity_id,
    location_id,
    weather_date,
    city,
    state,
    country,
    market,
    region,
    latitude,
    longitude,
    temperature_2m_mean,
    temperature_2m_max,
    temperature_2m_min,
    relative_humidity_2m_mean,
    precipitation_sum,
    weather_code,
    wind_speed_10m_mean,
    wind_speed_10m_max,
    source_system,
    event_type
)
VALUES (
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s,
    %s
)
AS new
ON DUPLICATE KEY UPDATE
    entity_id = new.entity_id,
    location_id = new.location_id,
    weather_date = new.weather_date,
    city = new.city,
    state = new.state,
    country = new.country,
    market = new.market,
    region = new.region,
    latitude = new.latitude,
    longitude = new.longitude,
    temperature_2m_mean =
        new.temperature_2m_mean,
    temperature_2m_max =
        new.temperature_2m_max,
    temperature_2m_min =
        new.temperature_2m_min,
    relative_humidity_2m_mean =
        new.relative_humidity_2m_mean,
    precipitation_sum =
        new.precipitation_sum,
    weather_code =
        new.weather_code,
    wind_speed_10m_mean =
        new.wind_speed_10m_mean,
    wind_speed_10m_max =
        new.wind_speed_10m_max,
    source_system =
        new.source_system,
    event_type =
        new.event_type
"""


# ============================================================
# KAFKA CONSUMER
# ============================================================

try:

    consumer = KafkaConsumer(
        TOPIC,

        bootstrap_servers=
            KAFKA_BROKER,

        group_id=
            CONSUMER_GROUP,

        # New group intentionally scans the topic.
        # Non-historical events are ignored.
        auto_offset_reset=
            "earliest",

        enable_auto_commit=
            False,

        value_deserializer=lambda value:
            json.loads(
                value.decode("utf-8")
            ),
    )

except Exception as error:

    print(
        f"Kafka consumer creation failed: "
        f"{error}"
    )

    cursor.close()
    connection.close()

    raise SystemExit(1)


# ============================================================
# START
# ============================================================

print("=" * 70)
print("Historical Weather MySQL Consumer Started")
print(f"Kafka : {KAFKA_BROKER}")
print(f"Topic : {TOPIC}")
print(
    "Target: WEATHER_HISTORICAL only"
)
print(
    "MySQL : localhost:3306/"
    "supply_chain_db.weather"
)
print("=" * 70)

print(
    "\nWaiting for historical weather "
    "events..."
)

print(
    "All other Kafka event types "
    "will be ignored.\n"
)


processed = 0
ignored = 0
errors = 0


# ============================================================
# PROCESS EVENTS
# ============================================================

try:

    for message in consumer:

        event = message.value

        event_type = event.get(
            "event_type"
        )


        # ----------------------------------------------------
        # IGNORE EVERYTHING EXCEPT HISTORICAL WEATHER
        # ----------------------------------------------------

        if event_type != TARGET_EVENT_TYPE:

            ignored += 1

            # Commit ignored messages too so this
            # dedicated group advances through old data.
            consumer.commit()

            if (
                ignored == 1
                or ignored % 50000 == 0
            ):

                print(
                    f"Scanning old Kafka data... "
                    f"Ignored: {ignored:,}"
                )

            continue


        # ----------------------------------------------------
        # HISTORICAL WEATHER EVENT
        # ----------------------------------------------------

        payload = event.get(
            "payload",
            {}
        )


        values = (

            event.get(
                "event_id"
            ),

            event.get(
                "entity_id"
            ),

            payload.get(
                "location_id"
            ),

            payload.get(
                "weather_date"
            ),

            payload.get(
                "city"
            ),

            payload.get(
                "state"
            ),

            payload.get(
                "country"
            ),

            payload.get(
                "market"
            ),

            payload.get(
                "region"
            ),

            payload.get(
                "latitude"
            ),

            payload.get(
                "longitude"
            ),

            payload.get(
                "temperature_2m_mean"
            ),

            payload.get(
                "temperature_2m_max"
            ),

            payload.get(
                "temperature_2m_min"
            ),

            payload.get(
                "relative_humidity_2m_mean"
            ),

            payload.get(
                "precipitation_sum"
            ),

            payload.get(
                "weather_code"
            ),

            payload.get(
                "wind_speed_10m_mean"
            ),

            payload.get(
                "wind_speed_10m_max"
            ),

            event.get(
                "source_system"
            ),

            event_type,
        )


        try:

            cursor.execute(
                INSERT_QUERY,
                values,
            )

            connection.commit()

            # Commit Kafka only after
            # successful MySQL write.
            consumer.commit()

            processed += 1


            if (
                processed == 1
                or processed % 1000 == 0
            ):

                print(
                    f"Historical weather "
                    f"processed: "
                    f"{processed:,}"
                )


        except mysql.connector.Error as error:

            connection.rollback()

            errors += 1

            print(
                "\nMYSQL ERROR"
            )

            print(
                f"Event ID: "
                f"{event.get('event_id')}"
            )

            print(
                f"Error: {error}"
            )

            # Do not commit this Kafka offset.
            # Stop instead of silently losing data.
            raise


except KeyboardInterrupt:

    print(
        "\nHistorical weather consumer "
        "stopped by user."
    )


finally:

    print("\n" + "=" * 70)
    print("FINAL CONSUMER SUMMARY")
    print("=" * 70)

    print(
        f"Historical processed : "
        f"{processed:,}"
    )

    print(
        f"Other events ignored : "
        f"{ignored:,}"
    )

    print(
        f"Errors               : "
        f"{errors:,}"
    )

    print("=" * 70)

    consumer.close()
    cursor.close()
    connection.close()