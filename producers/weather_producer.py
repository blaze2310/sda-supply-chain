import json, time
from pathlib import Path

import pandas as pd
import requests
from kafka import KafkaProducer


# ---------- CONFIG ----------
API = "https://power.larc.nasa.gov/api/temporal/daily/point"
ORDERS = Path("data/cleaned/orders/orders_cleaned.xlsx")
OUTPUT = Path("data/api/weather/historical_weather_data.xlsx")
CACHE = Path("data/api/weather/nasa_power_cache")
TOPIC = "supply_chain_events"

CACHE.mkdir(parents=True, exist_ok=True)
OUTPUT.parent.mkdir(parents=True, exist_ok=True)

PARAMS = "T2M,T2M_MAX,T2M_MIN,RH2M,PRECTOTCORR,WS10M,WS10M_MAX"

mapping = {
    "T2M": "temperature_2m_mean",
    "T2M_MAX": "temperature_2m_max",
    "T2M_MIN": "temperature_2m_min",
    "RH2M": "relative_humidity_2m_mean",
    "PRECTOTCORR": "precipitation_sum",
    "WS10M": "wind_speed_10m_mean",
    "WS10M_MAX": "wind_speed_10m_max",
}


# ---------- EXACT REQUIREMENTS FROM ORDERS ----------
orders = pd.read_excel(ORDERS)
orders["weather_date"] = pd.to_datetime(orders["order_date"]).dt.date

cols = [
    "location_id", "weather_date", "city", "state", "country",
    "market", "region", "latitude", "longitude"
]

req = orders[cols].drop_duplicates(["location_id", "weather_date"])

if len(req) != 21376 or req["location_id"].nunique() != 359:
    raise RuntimeError(
        f"SAFETY STOP: expected 21,376 rows / 359 locations, "
        f"found {len(req):,} / {req['location_id'].nunique():,}"
    )

print(f"Required records : {len(req):,}")
print(f"Locations        : {req['location_id'].nunique():,}")
print(f"Date range       : {req.weather_date.min()} -> {req.weather_date.max()}\n")


# ---------- NASA POWER ----------
session = requests.Session()
frames = []

for n, (location_id, group) in enumerate(req.groupby("location_id"), 1):

    info = group.iloc[0]
    required_dates = set(group["weather_date"])
    cache_file = CACHE / f"{location_id}.csv"

    print(f"[{n}/359] {location_id} | {info['city']}")

    # Use valid cache if already downloaded
    if cache_file.exists():
        df = pd.read_csv(cache_file)
        df["weather_date"] = pd.to_datetime(df["weather_date"]).dt.date

        if required_dates.issubset(set(df["weather_date"])):
            print("    CACHE OK")
        else:
            df = None
    else:
        df = None

    # Download if no complete cache
    if df is None:
        params = {
            "parameters": PARAMS,
            "community": "AG",
            "longitude": float(info["longitude"]),
            "latitude": float(info["latitude"]),
            "start": min(required_dates).strftime("%Y%m%d"),
            "end": max(required_dates).strftime("%Y%m%d"),
            "format": "JSON",
            "time-standard": "UTC",
        }

        # Retry temporary errors
        for attempt in range(5):
            try:
                r = session.get(API, params=params, timeout=120)
                r.raise_for_status()
                data = r.json()["properties"]["parameter"]
                break
            except Exception as e:
                if attempt == 4:
                    raise RuntimeError(f"{location_id} failed: {e}")
                wait = 15 * (attempt + 1)
                print(f"    Retry in {wait}s...")
                time.sleep(wait)

        dates = sorted(data["T2M"])
        df = pd.DataFrame({"weather_date": pd.to_datetime(dates).date})

        for nasa_name, column_name in mapping.items():
            df[column_name] = [data[nasa_name].get(d) for d in dates]

        df.to_csv(cache_file, index=False)
        print(f"    Downloaded {len(df):,} daily records")
        time.sleep(2)

    # Keep only dates actually required by Orders
    df = df[df["weather_date"].isin(required_dates)].copy()

    if set(df["weather_date"]) != required_dates:
        raise RuntimeError(f"SAFETY STOP: missing dates for {location_id}")

    # Add finalized location metadata
    for c in ["city", "state", "country", "market", "region", "latitude", "longitude"]:
        df[c] = info[c]

    df["location_id"] = location_id
    df["weather_code"] = None
    frames.append(df)


# ---------- FINAL DATASET ----------
weather = pd.concat(frames, ignore_index=True)

final_cols = [
    "location_id", "weather_date", "city", "state", "country",
    "market", "region", "latitude", "longitude",
    "temperature_2m_mean", "temperature_2m_max", "temperature_2m_min",
    "relative_humidity_2m_mean", "precipitation_sum", "weather_code",
    "wind_speed_10m_mean", "wind_speed_10m_max"
]

weather = weather[final_cols].sort_values(["location_id", "weather_date"])

pairs = weather[["location_id", "weather_date"]].drop_duplicates()

if len(weather) != 21376 or len(pairs) != 21376:
    raise RuntimeError(
        f"SAFETY STOP: expected 21,376 unique records, found "
        f"{len(weather):,} rows / {len(pairs):,} unique pairs. "
        "Nothing sent to Kafka."
    )

weather.to_excel(OUTPUT, index=False)

print("\nVALIDATION PASSED")
print(f"Records   : {len(weather):,}")
print(f"Locations : {weather.location_id.nunique():,}")
print(f"Dates     : {weather.weather_date.min()} -> {weather.weather_date.max()}")
print(f"Saved     : {OUTPUT}")


# ---------- KAFKA ----------
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda x: json.dumps(x, default=str).encode(),
    acks="all",
)

for n, (_, row) in enumerate(weather.iterrows(), 1):

    date = pd.to_datetime(row["weather_date"]).date()
    value = lambda x: None if pd.isna(x) else float(x)

    payload = {
        "location_id": row["location_id"],
        "weather_date": date.isoformat(),
        "city": row["city"],
        "state": row["state"],
        "country": row["country"],
        "market": row["market"],
        "region": row["region"],
        "latitude": value(row["latitude"]),
        "longitude": value(row["longitude"]),
        "temperature_2m_mean": value(row["temperature_2m_mean"]),
        "temperature_2m_max": value(row["temperature_2m_max"]),
        "temperature_2m_min": value(row["temperature_2m_min"]),
        "relative_humidity_2m_mean": value(row["relative_humidity_2m_mean"]),
        "precipitation_sum": value(row["precipitation_sum"]),
        "weather_code": None,
        "wind_speed_10m_mean": value(row["wind_speed_10m_mean"]),
        "wind_speed_10m_max": value(row["wind_speed_10m_max"]),
    }

    event = {
        "event_id": f"WEATHER-{row['location_id']}-{date:%Y%m%d}",
        "event_type": "WEATHER_HISTORICAL",
        "source_system": "nasa_power",
        "entity_id": row["location_id"],
        "event_timestamp": f"{date}T00:00:00",
        "payload": payload,
    }

    producer.send(TOPIC, value=event)

    if n == 1 or n % 1000 == 0:
        print(f"Kafka: {n:,}/21,376")

producer.flush()
producer.close()

print("\nCOMPLETE: 21,376 historical weather events published.")