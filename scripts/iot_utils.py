import json
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import requests

_BASE_DIR = Path(__file__).resolve().parent
_SECRETS_PATH = _BASE_DIR.parent / "config" / "secrets.json"
_SETTINGS_PATH = _BASE_DIR.parent / "config" / "settings.json"
REQUEST_TIMEOUT = 30
VERIFY_SSL = False  # change to True for proper TLS validation
_config_cache: dict[str, Any] | None = None


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
        read_key, request_timeout, verify_ssl, collection_id_tae
    """
    if secrets_path is None:
        secrets_path = _SECRETS_PATH
    if settings_path is None:
        settings_path = _SETTINGS_PATH
    
    secrets = _load_json_file(secrets_path, "secrets")
    cfg = _load_json_file(settings_path, "settings")
    
    return {
        "project_id": cfg.get("PROJECT_ID", ""),
        "base_url": cfg.get("BASE_URL", "").rstrip("/"),
        "master_key": secrets.get("master"),
        "write_key": secrets.get("write"),
        "read_key": secrets.get("read"),
        "request_timeout": _as_int(cfg.get("REQUEST_TIMEOUT"), REQUEST_TIMEOUT),
        "verify_ssl": _as_bool(cfg.get("VERIFY_SSL"), VERIFY_SSL),
        "collection_id_tae": cfg.get("collection_id_tae", cfg.get("COLLECTION_ID", "")),
    }


def _get_cached_config() -> dict[str, Any]:
    """Get cached config, loading from files once if needed."""
    global _config_cache
    if _config_cache is None:
        _config_cache = load_config()
    return _config_cache


def _request(method: str,
             url: str,
             **kwargs) -> requests.Response | None:
    try:
        return requests.request(
            method=method,
            url=url,
            timeout=REQUEST_TIMEOUT,
            verify=VERIFY_SSL,
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


def list_collections(project_id: str | None = None,
                     api_key: str | None = None,
                     base_url: str | None = None):
    """List collections for a project.
    
    Args:
        project_id: Project ID (defaults to PROJECT_ID from config)
        api_key: API key for authentication (defaults to master key from config)
        base_url: Base URL (defaults to BASE_URL from config)
    """
    if project_id is None or api_key is None or base_url is None:
        config = _get_cached_config()
        project_id = project_id or config.get("project_id", "")
        api_key = api_key or config.get("master_key")
        base_url = base_url or config.get("base_url", "")
    
    if not api_key:
        print("Missing API key for listing collections")
        return None

    url = f"{base_url}/projects/{project_id}/collections"
    headers = {"X-API-Key": api_key}

    response = _request("GET", url, headers=headers)
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
                      base_url: str | None = None):
    """Delete an entire collection.
    
    Args:
        project_id: Project ID
        collection_id: Collection ID to delete
        api_key: Master API key (defaults to master key from config)
        base_url: Base URL (defaults to BASE_URL from config)
    """
    if api_key is None or base_url is None:
        config = _get_cached_config()
        api_key = api_key or config.get("master_key")
        base_url = base_url or config.get("base_url", "")
    
    if not api_key:
        print("Missing API key for deleting collection")
        return None

    url = f"{base_url}/projects/{project_id}/collections/{collection_id}"
    headers = {"X-API-Key": api_key}

    response = _request("DELETE", url, headers=headers)
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
              base_url: str | None = None):
    """Send data (list of records) to a collection.
    
    Args:
        project_id: Project ID
        collection_id: Collection ID
        write_key: Write API key (defaults to write key from config)
        data: List of records to send
        base_url: Base URL (defaults to BASE_URL from config)
    """
    if data is None:
        data = []
    if write_key is None or base_url is None:
        config = _get_cached_config()
        write_key = write_key or config.get("write_key")
        base_url = base_url or config.get("base_url", "")
    
    if not write_key:
        print("Missing write key for send_data")
        return False

    url = f"{base_url}/projects/{project_id}/collections/{collection_id}/send_data"
    headers = {"X-API-Key": write_key}

    response = _request("POST", url, json=data, headers=headers)
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
                base_url: str | None = None):
    """Delete data from a collection based on criteria.
    
    Args:
        project_id: Project ID
        collection_id: Collection ID
        api_key: Master API key (defaults to master key from config)
        key: Filter by key
        timestamp_from: Delete data from this timestamp
        timestamp_to: Delete data until this timestamp
        base_url: Base URL (defaults to BASE_URL from config)
    """
    if api_key is None or base_url is None:
        config = _get_cached_config()
        api_key = api_key or config.get("master_key")
        base_url = base_url or config.get("base_url", "")
    
    if not api_key:
        print("Missing API key for delete_data")
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

    response = _request("DELETE", url, json=delete_request, headers=headers)
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
             base_url: str | None = None):
    """Get data from collection with optional filters.
    
    Args:
        project_id: Project ID
        collection_id: Collection ID
        read_key: Read API key (defaults to read key from config)
        filters: Optional filter list
        attributes: Optional attribute list
        limit: Limit number of results
        order_by: Order results by this attribute
        base_url: Base URL (defaults to BASE_URL from config)
    """
    if read_key is None or base_url is None:
        config = _get_cached_config()
        read_key = read_key or config.get("read_key")
        base_url = base_url or config.get("base_url", "")
    
    if not read_key:
        print("Missing read key for get_data")
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

    response = _request("GET", url, headers=headers, params=params)
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
                   base_url: str | None = None):
    """Get statistics for an attribute.
    
    Args:
        project_id: Project ID
        collection_id: Collection ID
        read_key: Read API key (defaults to read key from config)
        attribute: Attribute name
        stat: Statistic type (avg, min, max, sum, count, distinct, ...)
        filters: Optional filter list
        order: Order direction (asc/desc)
        interval: Time interval (every_24_hours, ...)
        base_url: Base URL (defaults to BASE_URL from config)
    """
    if read_key is None or base_url is None:
        config = _get_cached_config()
        read_key = read_key or config.get("read_key")
        base_url = base_url or config.get("base_url", "")
    
    if not read_key:
        print("Missing read key for get_statistics")
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

    response = _request("GET", url, headers=headers, params=params)
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
    base_url: str | None = None
):
    """Create a new collection in a project.
    
    Args:
        project_id: Project ID
        master_key: Master API key (defaults to master key from config)
        name: Collection name visible in dashboard
        description: Collection description
        tags: List of metadata tags
        schema: Dictionary describing the collection schema
        base_url: Base URL (defaults to BASE_URL from config)
    """
    if master_key is None or base_url is None:
        config = _get_cached_config()
        master_key = master_key or config.get("master_key")
        base_url = base_url or config.get("base_url", "")
    
    if not master_key:
        print("Missing master key for create_collection")
        return None

    url = f"{base_url}/projects/{project_id}/collections"
    headers = {"X-API-Key": master_key}

    payload = {
        "name": name,
        "description": description,
        "tags": tags or [],
        "collection_schema": schema or {},
    }

    response = _request("POST", url, json=payload, headers=headers)
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
