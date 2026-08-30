import json
import time

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

KAFKA_BROKER = "localhost:9092"
TOPIC = "supply_chain_events"
WEATHER_API_URL = "https://api.open-meteo.com/v1/forecast"
POLL_INTERVAL_SECONDS = 60

REGIONS = [
    {
        "market_clean": "APAC",
        "region_clean": "Central Asia",
        "reference_city": "Almaty",
        "latitude": 43.2389,
        "longitude": 76.8897,
    },
    {
        "market_clean": "APAC",
        "region_clean": "North Asia",
        "reference_city": "Beijing",
        "latitude": 39.9042,
        "longitude": 116.4074,
    },
    {
        "market_clean": "APAC",
        "region_clean": "Oceania",
        "reference_city": "Sydney",
        "latitude": -33.8688,
        "longitude": 151.2093,
    },
    {
        "market_clean": "APAC",
        "region_clean": "Southeast Asia",
        "reference_city": "Singapore",
        "latitude": 1.3521,
        "longitude": 103.8198,
    },
    {
        "market_clean": "Africa",
        "region_clean": "Africa",
        "reference_city": "Nairobi",
        "latitude": -1.2921,
        "longitude": 36.8219,
    },
    {
        "market_clean": "Canada",
        "region_clean": "Canada",
        "reference_city": "Toronto",
        "latitude": 43.6532,
        "longitude": -79.3832,
    },
    {
        "market_clean": "EMEA",
        "region_clean": "EMEA",
        "reference_city": "Dubai",
        "latitude": 25.2048,
        "longitude": 55.2708,
    },
    {
        "market_clean": "EU",
        "region_clean": "Central",
        "reference_city": "Frankfurt",
        "latitude": 50.1109,
        "longitude": 8.6821,
    },
    {
        "market_clean": "EU",
        "region_clean": "North",
        "reference_city": "Stockholm",
        "latitude": 59.3293,
        "longitude": 18.0686,
    },
    {
        "market_clean": "EU",
        "region_clean": "South",
        "reference_city": "Rome",
        "latitude": 41.9028,
        "longitude": 12.4964,
    },
    {
        "market_clean": "LATAM",
        "region_clean": "Caribbean",
        "reference_city": "Santo Domingo",
        "latitude": 18.4861,
        "longitude": -69.9312,
    },
    {
        "market_clean": "LATAM",
        "region_clean": "Central",
        "reference_city": "Mexico City",
        "latitude": 19.4326,
        "longitude": -99.1332,
    },
    {
        "market_clean": "LATAM",
        "region_clean": "North",
        "reference_city": "Monterrey",
        "latitude": 25.6866,
        "longitude": -100.3161,
    },
    {
        "market_clean": "LATAM",
        "region_clean": "South",
        "reference_city": "Sao Paulo",
        "latitude": -23.5505,
        "longitude": -46.6333,
    },
    {
        "market_clean": "US",
        "region_clean": "Central",
        "reference_city": "Chicago",
        "latitude": 41.8781,
        "longitude": -87.6298,
    },
    {
        "market_clean": "US",
        "region_clean": "East",
        "reference_city": "New York",
        "latitude": 40.7128,
        "longitude": -74.0060,
    },
    {
        "market_clean": "US",
        "region_clean": "South",
        "reference_city": "Dallas",
        "latitude": 32.7767,
        "longitude": -96.7970,
    },
    {
        "market_clean": "US",
        "region_clean": "West",
        "reference_city": "Los Angeles",
        "latitude": 34.0522,
        "longitude": -118.2437,
    },
]


def make_region_id(market, region):
    value = f"{market}-{region}".upper()
    return "WEATHER-" + value.replace(" ", "-")


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


latitudes = ",".join(str(region["latitude"]) for region in REGIONS)
longitudes = ",".join(str(region["longitude"]) for region in REGIONS)

params = {
    "latitude": latitudes,
    "longitude": longitudes,
    "current": (
        "temperature_2m,relative_humidity_2m,"
        "precipitation,weather_code,"
        "wind_speed_10m,wind_direction_10m"
    ),
    "timezone": "auto",
}

cycle_number = 1

print(f"Streaming live weather for {len(REGIONS)} regions...")
print("Press Control + C to stop.\n")

try:
    while True:
        response = requests.get(
            WEATHER_API_URL,
            params=params,
            timeout=30,
        )
        response.raise_for_status()

        weather_results = response.json()

        if isinstance(weather_results, dict):
            weather_results = [weather_results]

        for index, (region, result) in enumerate(
            zip(REGIONS, weather_results),
            start=1,
        ):
            current = result["current"]

            weather_region_id = make_region_id(
                region["market_clean"],
                region["region_clean"],
            )

            event = {
                "event_id": (
                    f"WEATHER-EVENT-{cycle_number:04d}-{index:03d}"
                ),
                "event_type": "WEATHER_OBSERVED",
                "source_system": "open_meteo",
                "entity_id": weather_region_id,
                "payload": {
                    "weather_region_id": weather_region_id,
                    "market_clean": region["market_clean"],
                    "region_clean": region["region_clean"],
                    "reference_city": region["reference_city"],
                    "latitude": region["latitude"],
                    "longitude": region["longitude"],
                    "temperature_c": current.get("temperature_2m"),
                    "relative_humidity_pct": current.get(
                        "relative_humidity_2m"
                    ),
                    "precipitation_mm": current.get("precipitation"),
                    "weather_code": current.get("weather_code"),
                    "wind_speed_kmh": current.get("wind_speed_10m"),
                    "wind_direction_deg": current.get(
                        "wind_direction_10m"
                    ),
                },
            }

            producer.send(TOPIC, value=event)

            print(
                f"Sent {event['event_id']} | "
                f"entity={weather_region_id} | "
                f"city={region['reference_city']} | "
                f"temperature={current.get('temperature_2m')} C"
            )

        producer.flush()

        print(
            f"\nCycle {cycle_number} completed. "
            f"Next update in {POLL_INTERVAL_SECONDS} seconds.\n"
        )

        cycle_number += 1
        time.sleep(POLL_INTERVAL_SECONDS)

except KeyboardInterrupt:
    print("\nWeather streaming stopped.")

except requests.RequestException as error:
    print(f"Weather API request failed: {error}")

finally:
    producer.flush()
    producer.close()