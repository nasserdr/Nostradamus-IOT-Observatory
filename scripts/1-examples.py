import json
import random
from datetime import datetime, timedelta
from pathlib import Path

import requests

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

# You can adapt these paths if needed
BASE_DIR = Path(__file__).resolve().parent
SECRETS_PATH = BASE_DIR.parent / "config" / "secrets.json"
SETTINGS_PATH = BASE_DIR.parent / "config" / "settings.json"

with open(SECRETS_PATH, "r", encoding="utf-8") as f:
    secrets = json.load(f)

with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
    cfg = json.load(f)

print(secrets)
print(cfg)
PROJECT_ID = cfg["PROJECT_ID"]
BASE_URL = cfg["BASE_URL"].rstrip("/")

MASTER_KEY = secrets.get("master")  # admin-level operations (collections, delete)
WRITE_KEY = secrets.get("write")    # sending data
READ_KEY = secrets.get("read")      # reading data

# Set this to True once your certificates / proxy are configured correctly
VERIFY_SSL = False   # current environment: corporate proxy => warnings
                     # change to True for proper TLS validation

# example_usage.py
from utils import *

# 👇 Replace with your actual collection ID
COLLECTION_ID = "YOUR_COLLECTION_ID"


def demo_list_collections():
    print("\n=== List collections ===")
    list_collections()


def demo_send_synthetic_data():
    print("\n=== Send synthetic soil data ===")
    data = generate_synthetic_data(num_sensors=3, num_readings=24)
    send_data(PROJECT_ID, COLLECTION_ID, WRITE_KEY, data)


def demo_get_data_filters():
    print("\n=== Get data with filters ===")

    # 1) Filter by sensor key SOIL_001
    print("\n🔍 Filter by sensor SOIL_001...")
    sensor_filter = [
        {
            "property_name": "key",
            "operator": "eq",
            "property_value": "SOIL_001",
        }
    ]
    sensor_data = get_data(
        PROJECT_ID,
        COLLECTION_ID,
        READ_KEY,
        filters=sensor_filter,
        order_by="timestamp asc",
    )
    print(f"Found {len(sensor_data)} records for SOIL_001")

    # 2) Filter for high moisture (> 45%)
    print("\n💧 Filter for high moisture (>45%)...")
    moisture_filter = [
        {
            "property_name": "soil_moisture",
            "operator": "gt",
            "property_value": 45,
        }
    ]
    high_moisture = get_data(
        PROJECT_ID,
        COLLECTION_ID,
        READ_KEY,
        filters=moisture_filter,
    )
    print(f"Found {len(high_moisture)} high-moisture records")

    # 3) Complex filter - low moisture AND low battery
    print("\n⚠️ Filter for low moisture AND low battery...")
    complex_filter = [
        {
            "property_name": "soil_moisture",
            "operator": "lt",
            "property_value": 35,
        },
        {
            "property_name": "battery_level",
            "operator": "lt",
            "property_value": 80,
        },
    ]
    alert_data = get_data(
        PROJECT_ID,
        COLLECTION_ID,
        READ_KEY,
        filters=complex_filter,
    )
    print(f"Found {len(alert_data)} alert records")


def demo_statistics():
    print("\n=== Statistics examples ===")

    # Average soil moisture
    print("\n📈 Average soil moisture...")
    get_statistics(
        PROJECT_ID,
        COLLECTION_ID,
        READ_KEY,
        attribute="soil_moisture",
        stat="avg",
    )

    # Max temperature
    print("\n🔥 Maximum temperature...")
    get_statistics(
        PROJECT_ID,
        COLLECTION_ID,
        READ_KEY,
        attribute="temperature",
        stat="max",
        order="desc",
    )

    # Min battery level for SOIL_001
    print("\n🔋 Minimum battery level for SOIL_001...")
    battery_filter = [
        {
            "property_name": "key",
            "operator": "eq",
            "property_value": "SOIL_001",
        }
    ]
    get_statistics(
        PROJECT_ID,
        COLLECTION_ID,
        READ_KEY,
        attribute="battery_level",
        stat="min",
        filters=battery_filter,
    )

    # Distinct sensor keys
    print("\n🔑 Distinct sensor keys...")
    get_statistics(
        PROJECT_ID,
        COLLECTION_ID,
        READ_KEY,
        attribute="key",
        stat="distinct",
    )


def demo_delete_data():
    print("\n=== Delete data example ===")

    # Example: delete data for SOIL_001 for a specific day
    delete_data(
        PROJECT_ID,
        COLLECTION_ID,
        api_key=MASTER_KEY,
        key="SOIL_001",
        timestamp_from="2025-09-18T00:00:00Z",
        timestamp_to="2025-09-18T23:00:00Z",
    )


def demo_delete_collection():
    print("\n=== Delete entire collection (DANGEROUS) ===")
    delete_collection(
        PROJECT_ID,
        COLLECTION_ID,
        api_key=MASTER_KEY,
    )

def demo_create_collection():
    print("\n=== Create soil collection ===")

    weather_schema = {
        # optional common fields (uncomment if you use them)
        "key": "TAE",
        "timestamp": "",
        "air-temperature_celsius": "",
        "air-pressure_mbar": "",
        "air-humidity_percent": "",
        "vapor-pressure_mbar": "",
        "wind-speed_m_s": "",
        "wind-direction_angle": "",
        "wind-gust_m_s": "",
        "gust-peak_m_s": "",
        "solar-radiation_w_m2": "",
        "precipitation_mm": "",
        "dew-point_celsius": "",
        "latitude_4326": "",
        "longitude_4326": "",
    }

    result = create_collection(
        project_id=PROJECT_ID,
        master_key=MASTER_KEY,
        name="meteoswiss_tae",
        description="Weather data from MeteoSwiss TAE (Tanikon) station, 50 meters away from the Swiss Future Farm (The Swiss Pilot Site for Nostradamus)",
        tags=["weather", "meteoswiss", "switzerland"],
        schema=weather_schema
    )

    print(result)


if __name__ == "__main__":
    # 👇 Uncomment what you want to test


    # demo_list_collections()
    demo_create_collection()
    # demo_list_collections()
    # demo_send_synthetic_data()
    # demo_get_data_filters()
    # demo_statistics()
    # demo_delete_data()
    # COLLECTION_ID = 'd24ddf0b-7a94-4556-877d-50da43935b30'
    # demo_delete_collection()  
    pass
