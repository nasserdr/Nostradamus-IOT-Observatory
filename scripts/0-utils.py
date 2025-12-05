import json
import random
from datetime import datetime, timedelta
from pathlib import Path

import requests

with open("config/secrets.json", "r", encoding="utf-8") as f:
    secrets = json.load(f)

with open("config/settings.json", "r", encoding="utf-8") as f:
    cfg = json.load(f)
PROJECT_ID = cfg["PROJECT_ID"]
BASE_URL   = cfg["BASE_URL"].rstrip("/")
MASTER_KEY = secrets.get("master")  # only needed if you want to create the collection
WRITE_KEY  = secrets.get("write")   # ← use 'write' for sending data
READ_KEY   = secrets.get("read")    # not used here
VERIFY_SSL = False   # change to True for proper TLS validation
# ---------------------------------------------------------------------
# Collections utilities
# ---------------------------------------------------------------------

def list_collections(project_id: str = PROJECT_ID,
                     api_key: str = MASTER_KEY):
    """List collections for a project."""
    url = f"{BASE_URL}/projects/{project_id}/collections"
    headers = {"X-API-Key": api_key}

    response = requests.get(url, headers=headers, verify=VERIFY_SSL)

    if response.status_code == 200:
        collections = response.json()
        print("📦 Available Collections:")
        for col in collections:
            print(
                f"- ID: {col.get('collection_id')} | "
                f"Name: {col.get('collection_name')} | "
                f"Description: {col.get('description')}"
            )
        return collections
    else:
        print("Failed to list collections:", response.text)
        return None


def delete_collection(project_id: str,
                      collection_id: str,
                      api_key: str = MASTER_KEY):
    """Delete an entire collection."""
    url = f"{BASE_URL}/projects/{project_id}/collections/{collection_id}"
    headers = {"X-API-Key": api_key}

    response = requests.delete(url, headers=headers, verify=VERIFY_SSL)

    if response.status_code == 200:
        try:
            result = response.json()
            print(f"🗑️ Collection deleted successfully: "
                  f"{result.get('message', 'No message returned')}")
            return result
        except Exception:
            print("🗑️ Collection deleted successfully (no JSON body).")
            return True
    else:
        print(f"Failed to delete collection: {response.text}")
        return None


# ---------------------------------------------------------------------
# Data CRUD utilities
# ---------------------------------------------------------------------

def send_data(project_id: str,
              collection_id: str,
              write_key: str,
              data: list[dict]):
    """Send data (list of records) to a collection."""
    url = f"{BASE_URL}/projects/{project_id}/collections/{collection_id}/send_data"
    headers = {"X-API-Key": write_key}

    response = requests.post(url, json=data, headers=headers, verify=VERIFY_SSL)
    if response.status_code == 200:
        print(f"✅ Sent {len(data)} records")
        return True
    else:
        print(f"Failed to send data: {response.text}")
        return False


def delete_data(project_id: str,
                collection_id: str,
                api_key: str = MASTER_KEY,
                key: str | None = None,
                timestamp_from: str | None = None,
                timestamp_to: str | None = None):
    """Delete data from a collection based on criteria."""
    url = f"{BASE_URL}/projects/{project_id}/collections/{collection_id}/delete_data"
    headers = {"X-API-Key": api_key}

    delete_request: dict = {}
    if key:
        delete_request["key"] = key
    if timestamp_from:
        delete_request["timestamp_from"] = timestamp_from
    if timestamp_to:
        delete_request["timestamp_to"] = timestamp_to

    response = requests.delete(
        url,
        json=delete_request,
        headers=headers,
        verify=VERIFY_SSL,
    )

    if response.status_code == 200:
        result = response.json()
        print(f"✅ Data deleted successfully: {result.get('message')}")
        return result
    else:
        print(f"Failed to delete data: {response.text}")
        return None


def get_data(project_id: str,
             collection_id: str,
             read_key: str,
             filters: list[dict] | None = None,
             attributes: list[str] | None = None,
             limit: int | None = None,
             order_by: str | None = None):
    """Get data from collection with optional filters."""
    url = f"{BASE_URL}/projects/{project_id}/collections/{collection_id}/get_data"
    headers = {"X-API-Key": read_key}
    params: dict = {}

    if order_by:
        params["order_by"] = order_by
    if attributes:
        # API usually expects comma-separated list for attributes
        params["attributes"] = ",".join(attributes)
    if limit:
        params["limit"] = limit
    if filters:
        params["filters"] = json.dumps(filters)

    response = requests.get(url, headers=headers, params=params, verify=VERIFY_SSL)

    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print(f"Failed to get data: {response.text}")
        return []


def get_statistics(project_id: str,
                   collection_id: str,
                   read_key: str,
                   attribute: str,
                   stat: str = "avg",
                   filters: list[dict] | None = None,
                   order: str = "asc",
                   interval: str = "every_24_hours"):
    """
    Get statistics for an attribute.

    stat: "avg", "min", "max", "sum", "count", "distinct", ...
    """
    url = f"{BASE_URL}/projects/{project_id}/collections/{collection_id}/statistics"
    headers = {"X-API-Key": read_key}

    params: dict = {
        "attribute": attribute,
        "stat": stat,
        "interval": interval,
        "order": order,
    }

    if filters:
        params["filters"] = json.dumps(filters)

    response = requests.get(url, headers=headers, params=params, verify=VERIFY_SSL)

    if response.status_code == 200:
        stats = response.json()
        print(f"✅ {stat} for {attribute}: {stats}")
        return stats
    else:
        print(f"Failed to get statistics: {response.text}")
        return {}


# ---------------------------------------------------------------------
# Synthetic data generator
# ---------------------------------------------------------------------

def generate_synthetic_data(num_sensors: int = 3,
                            num_readings: int = 24):
    """
    Generate synthetic soil sensor data for testing.

    num_sensors: number of sensor IDs (SOIL_001, SOIL_002, ...)
    num_readings: number of readings per sensor
    """
    data: list[dict] = []
    base_time = datetime.now() - timedelta(hours=24)

    for sensor_num in range(1, num_sensors + 1):
        sensor_id = f"SOIL_{sensor_num:03d}"

        for hour in range(num_readings):
            timestamp = base_time + timedelta(hours=hour)

            reading = {
                "key": sensor_id,
                "timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%S"),
                "soil_moisture": round(random.uniform(30, 60), 1),
                "ph_level": round(random.uniform(6.0, 7.5), 1),
                "temperature": round(random.uniform(15, 25), 1),
                "nitrogen": round(random.uniform(30, 60), 1),
                "phosphorus": round(random.uniform(8, 20), 1),
                "potassium": round(random.uniform(120, 220), 1),
                "battery_level": round(random.uniform(70, 100), 1),
            }
            data.append(reading)

    return data

def create_collection(
    project_id: str,
    master_key: str,
    name: str,
    description: str = "",
    tags: list[str] | None = None,
    schema: dict | None = None
):
    """
    Create a new collection in a project.

    name:         Collection name visible in Nostradamus dashboard
    description:  Description text
    tags:         List of metadata tags
    schema:       Dictionary describing the collection schema
    """

    url = f"{BASE_URL}/projects/{project_id}/collections"
    headers = {"X-API-Key": master_key}

    payload = {
        "name": name,
        "description": description,
        "tags": tags or [],
        "collection_schema": schema or {},
    }

    response = requests.post(url, json=payload, headers=headers, verify=VERIFY_SSL)

    if response.status_code == 200:
        print("✅ Collection created successfully!")
        return response.json()
    else:
        print(f"Failed to create collection: {response.text}")
        return None
