import json
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import requests

DEFAULT_REQUEST_TIMEOUT = 30
DEFAULT_VERIFY_SSL = True


def _as_bool(value: Any, default: bool) -> bool:
    """Convert config values to bool safely."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "no", "n", "off"}:
            return False
    return default


def _as_int(value: Any, default: int) -> int:
    """Convert config values to int safely."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _load_json_file(path: Path, label: str) -> dict[str, Any]:
    """Load a JSON file with proper error handling."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError as exc:
        raise RuntimeError(f"Missing {label} file: {path}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON in {label} file: {path}") from exc
    except OSError as exc:
        raise RuntimeError(f"Failed to read {label} file: {path}") from exc


def load_config(secrets_path: Path | None = None, 
                settings_path: Path | None = None) -> dict[str, Any]:
    """
    Load configuration from JSON files.
    
    Args:
        secrets_path: Path to secrets.json (defaults to config/secrets.json)
        settings_path: Path to settings.json (defaults to config/settings.json)
    
    Returns:
        Dictionary with keys: project_id, base_url, master_key, write_key,
        read_key, request_timeout, verify_ssl
    """
    if secrets_path is None:
        print("No secrets_path provided")
        raise ValueError("secrets_path is required")
    if settings_path is None:
        print("No settings_path provided")
        raise ValueError("settings_path is required")

    secrets = _load_json_file(secrets_path, "secrets")
    cfg = _load_json_file(settings_path, "settings")
    
    return {
        "project_id": cfg.get("PROJECT_ID", ""),
        "base_url": cfg.get("BASE_URL", "").rstrip("/"),
        "master_key": secrets.get("master"),
        "write_key": secrets.get("write"),
        "read_key": secrets.get("read"),
        "request_timeout": _as_int(cfg.get("REQUEST_TIMEOUT"), DEFAULT_REQUEST_TIMEOUT),
        "verify_ssl": _as_bool(cfg.get("VERIFY_SSL"), DEFAULT_VERIFY_SSL)
    }


def _request(method: str,
             url: str,
             timeout: int,
             verify_ssl: bool,
             **kwargs) -> requests.Response | None:
    try:
        return requests.request(
            method=method,
            url=url,
            timeout=timeout,
            verify=verify_ssl,
            **kwargs,
        )
    except requests.RequestException as exc:
        print(f"Network error during {method} {url}: {exc}")
        return None


def _safe_json(response: requests.Response,
               context: str) -> Any | None:
    try:
        return response.json()
    except ValueError:
        print(f"Failed to decode JSON for {context}. Response was: {response.text}")
        return None


def validate_config(project_id: str, base_url: str) -> bool:
    """Validate required config values."""
    if not project_id:
        print("Missing PROJECT_ID in settings.json")
        return False
    if not base_url:
        print("Missing BASE_URL in settings.json")
        return False
    return True


def get_collection_id_from_station_code(
    station_code: str,
    project_id: str,
    api_key: str,
    base_url: str,
    timeout: int = DEFAULT_REQUEST_TIMEOUT,
    verify_ssl: bool = DEFAULT_VERIFY_SSL,
) -> str:
    """
    Get collection ID for a station code by searching available collections.

    Args:
        station_code: Station code to search for (e.g., 'TAE', 'tae')
        project_id: Project ID
        api_key: API key for authentication
        base_url: Base URL
        timeout: Request timeout in seconds
        verify_ssl: Whether to verify SSL certificates

    Returns:
        Collection ID matching the station code

    Raises:
        RuntimeError: If no collection found for the station code
    """
    collections = list_collections(
        project_id=project_id,
        api_key=api_key,
        base_url=base_url,
        timeout=timeout,
        verify_ssl=verify_ssl,
    )

    if not collections:
        raise RuntimeError(
            f"No collections found for project {project_id}. "
            f"Please first create a collection for that project."
        )
    station_code = "meteoswiss_" + station_code
    station_code_upper = station_code.upper()

    for col in collections:
        collection_name = col.get("collection_name", "").upper()
        collection_id = col.get("collection_id")

        # Match by collection name containing station code
        if station_code_upper in collection_name and collection_id:
            return collection_id

    # If no match found
    raise RuntimeError(
        f"No collection found for station code '{station_code}'. "
        f"Please first create a collection for '{station_code}'."
    )


def list_collections(project_id: str | None = None,
                     api_key: str | None = None,
                     base_url: str | None = None,
                     timeout: int = DEFAULT_REQUEST_TIMEOUT,
                     verify_ssl: bool = DEFAULT_VERIFY_SSL):
    """List collections for a project.
    
    Args:
        project_id: Project ID
        api_key: API key for authentication
        base_url: Base URL
    """
    if not project_id:
        print("Missing project_id for listing collections")
        return None
    if not api_key:
        print("Missing API key for listing collections")
        return None
    if not base_url:
        print("Missing base_url for listing collections")
        return None

    url = f"{base_url}/projects/{project_id}/collections"
    headers = {"X-API-Key": api_key}

    response = _request("GET", url, timeout=timeout, verify_ssl=verify_ssl, headers=headers)
    if response is None:
        return None

    if response.status_code == 200:
        collections = _safe_json(response, "list_collections")
        if not isinstance(collections, list):
            return None
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
                      api_key: str | None = None,
                      base_url: str | None = None,
                      timeout: int = DEFAULT_REQUEST_TIMEOUT,
                      verify_ssl: bool = DEFAULT_VERIFY_SSL):
    """Delete an entire collection.
    
    Args:
        project_id: Project ID
        collection_id: Collection ID to delete
        api_key: Master API key
        base_url: Base URL
    """
    if not project_id:
        print("Missing project_id for deleting collection")
        return None
    if not collection_id:
        print("Missing collection_id for deleting collection")
        return None
    if not api_key:
        print("Missing API key for deleting collection")
        return None
    if not base_url:
        print("Missing base_url for deleting collection")
        return None

    url = f"{base_url}/projects/{project_id}/collections/{collection_id}"
    headers = {"X-API-Key": api_key}

    response = _request("DELETE", url, timeout=timeout, verify_ssl=verify_ssl, headers=headers)
    if response is None:
        return None

    if response.status_code == 200:
        result = _safe_json(response, "delete_collection")
        if isinstance(result, dict):
            print(f"🗑️ Collection deleted successfully: "
                  f"{result.get('message', 'No message returned')}")
            return result
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
              write_key: str | None = None,
              data: list[dict] | None = None,
              base_url: str | None = None,
              timeout: int = DEFAULT_REQUEST_TIMEOUT,
              verify_ssl: bool = DEFAULT_VERIFY_SSL):
    """Send data (list of records) to a collection.
    
    Args:
        project_id: Project ID
        collection_id: Collection ID
        write_key: Write API key
        data: List of records to send
        base_url: Base URL
    """
    if data is None:
        data = []
    if not project_id:
        print("Missing project_id for send_data")
        return False
    if not collection_id:
        print("Missing collection_id for send_data")
        return False
    if not write_key:
        print("Missing write key for send_data")
        return False
    if not base_url:
        print("Missing base_url for send_data")
        return False

    url = f"{base_url}/projects/{project_id}/collections/{collection_id}/send_data"
    headers = {"X-API-Key": write_key}

    response = _request(
        "POST",
        url,
        timeout=timeout,
        verify_ssl=verify_ssl,
        json=data,
        headers=headers,
    )
    if response is None:
        return False

    if response.status_code == 200:
        print(f"✅ Sent {len(data)} records")
        return True
    else:
        print(f"Failed to send data: {response.text}")
        return False


def delete_data(project_id: str,
                collection_id: str,
                api_key: str | None = None,
                key: str | None = None,
                timestamp_from: str | None = None,
                timestamp_to: str | None = None,
                base_url: str | None = None,
                timeout: int = DEFAULT_REQUEST_TIMEOUT,
                verify_ssl: bool = DEFAULT_VERIFY_SSL):
    """Delete data from a collection based on criteria.
    
    Args:
        project_id: Project ID
        collection_id: Collection ID
        api_key: Master API key
        key: Filter by key
        timestamp_from: Delete data from this timestamp
        timestamp_to: Delete data until this timestamp
        base_url: Base URL
    """
    if not project_id:
        print("Missing project_id for delete_data")
        return None
    if not collection_id:
        print("Missing collection_id for delete_data")
        return None
    if not api_key:
        print("Missing API key for delete_data")
        return None
    if not base_url:
        print("Missing base_url for delete_data")
        return None

    url = f"{base_url}/projects/{project_id}/collections/{collection_id}/delete_data"
    headers = {"X-API-Key": api_key}

    delete_request: dict = {}
    if key:
        delete_request["key"] = key
    if timestamp_from:
        delete_request["timestamp_from"] = timestamp_from
    if timestamp_to:
        delete_request["timestamp_to"] = timestamp_to

    response = _request(
        "DELETE",
        url,
        timeout=timeout,
        verify_ssl=verify_ssl,
        json=delete_request,
        headers=headers,
    )
    if response is None:
        return None

    if response.status_code == 200:
        result = _safe_json(response, "delete_data")
        if not isinstance(result, dict):
            return None
        print(f"✅ Data deleted successfully: {result.get('message')}")
        return result
    else:
        print(f"Failed to delete data: {response.text}")
        return None


def get_data(project_id: str,
             collection_id: str,
             read_key: str | None = None,
             filters: list[dict] | None = None,
             attributes: list[str] | None = None,
             limit: int | None = None,
             order_by: str | None = None,
             base_url: str | None = None,
             timeout: int = DEFAULT_REQUEST_TIMEOUT,
             verify_ssl: bool = DEFAULT_VERIFY_SSL):
    """Get data from collection with optional filters.
    
    Args:
        project_id: Project ID
        collection_id: Collection ID
        read_key: Read API key
        filters: Optional filter list
        attributes: Optional attribute list
        limit: Limit number of results
        order_by: Order results by this attribute
        base_url: Base URL
    """
    if not project_id:
        print("Missing project_id for get_data")
        return []
    if not collection_id:
        print("Missing collection_id for get_data")
        return []
    if not read_key:
        print("Missing read key for get_data")
        return []
    if not base_url:
        print("Missing base_url for get_data")
        return []

    url = f"{base_url}/projects/{project_id}/collections/{collection_id}/get_data"
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

    response = _request(
        "GET",
        url,
        timeout=timeout,
        verify_ssl=verify_ssl,
        headers=headers,
        params=params,
    )
    if response is None:
        return []

    if response.status_code == 200:
        data = _safe_json(response, "get_data")
        if data is None:
            return []
        return data
    else:
        print(f"Failed to get data: {response.text}")
        return []


def get_statistics(project_id: str,
                   collection_id: str,
                   read_key: str | None = None,
                   attribute: str = "",
                   stat: str = "avg",
                   filters: list[dict] | None = None,
                   order: str = "asc",
                   interval: str = "every_24_hours",
                   base_url: str | None = None,
                   timeout: int = DEFAULT_REQUEST_TIMEOUT,
                   verify_ssl: bool = DEFAULT_VERIFY_SSL):
    """Get statistics for an attribute.
    
    Args:
        project_id: Project ID
        collection_id: Collection ID
        read_key: Read API key
        attribute: Attribute name
        stat: Statistic type (avg, min, max, sum, count, distinct, ...)
        filters: Optional filter list
        order: Order direction (asc/desc)
        interval: Time interval (every_24_hours, ...)
        base_url: Base URL
    """
    if not project_id:
        print("Missing project_id for get_statistics")
        return {}
    if not collection_id:
        print("Missing collection_id for get_statistics")
        return {}
    if not read_key:
        print("Missing read key for get_statistics")
        return {}
    if not base_url:
        print("Missing base_url for get_statistics")
        return {}

    url = f"{base_url}/projects/{project_id}/collections/{collection_id}/statistics"
    headers = {"X-API-Key": read_key}

    params: dict = {
        "attribute": attribute,
        "stat": stat,
        "interval": interval,
        "order": order,
    }

    if filters:
        params["filters"] = json.dumps(filters)

    response = _request(
        "GET",
        url,
        timeout=timeout,
        verify_ssl=verify_ssl,
        headers=headers,
        params=params,
    )
    if response is None:
        return {}

    if response.status_code == 200:
        stats = _safe_json(response, "get_statistics")
        if stats is None:
            return {}
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
    master_key: str | None = None,
    name: str = "",
    description: str = "",
    tags: list[str] | None = None,
    schema: dict | None = None,
    base_url: str | None = None,
    timeout: int = DEFAULT_REQUEST_TIMEOUT,
    verify_ssl: bool = DEFAULT_VERIFY_SSL,
):
    """Create a new collection in a project.
    
    Args:
        project_id: Project ID
        master_key: Master API key
        name: Collection name visible in dashboard
        description: Collection description
        tags: List of metadata tags
        schema: Dictionary describing the collection schema
        base_url: Base URL
    """
    if not project_id:
        print("Missing project_id for create_collection")
        return None
    if not master_key:
        print("Missing master key for create_collection")
        return None
    if not base_url:
        print("Missing base_url for create_collection")
        return None

    url = f"{base_url}/projects/{project_id}/collections"
    headers = {"X-API-Key": master_key}

    payload = {
        "name": name,
        "description": description,
        "tags": tags or [],
        "collection_schema": schema or {},
    }

    response = _request(
        "POST",
        url,
        timeout=timeout,
        verify_ssl=verify_ssl,
        json=payload,
        headers=headers,
    )
    if response is None:
        return None

    if response.status_code == 200:
        print("✅ Collection created successfully!")
        result = _safe_json(response, "create_collection")
        if isinstance(result, dict):
            return result
        return None
    else:
        print(f"Failed to create collection: {response.text}")
        return None
