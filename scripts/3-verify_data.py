#!/usr/bin/env python
import json
import sys
from datetime import datetime
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SETTINGS_PATH = "config/settings.json"
SECRETS_PATH = "config/secrets.json"

VERIFY_SSL = False  # adjust when proxy/cert is configured


# ───────────────────────────────────────────────────────────────
# Helper: Load config
# ───────────────────────────────────────────────────────────────
def load_config():
    with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
        settings = json.load(f)
    with open(SECRETS_PATH, "r", encoding="utf-8") as f:
        secrets = json.load(f)
    return settings, secrets


# ───────────────────────────────────────────────────────────────
# Get collection ID for the station
# ───────────────────────────────────────────────────────────────
def get_collection_id(settings: dict, station_code: str):
    key = f"collection_id_{station_code.lower()}"
    coll_id = settings.get(key)
    if not coll_id:
        print(f"❌ No collection ID found in settings.json under key: '{key}'")
    return coll_id


# ───────────────────────────────────────────────────────────────
# Fetch data from Nostradamus API
# ───────────────────────────────────────────────────────────────
def fetch_collection_data(project_id: str, collection_id: str, read_key: str, limit=1000):
    """Fetch up to 'limit' rows to inspect correctness."""
    url = f"{BASE_URL}/projects/{project_id}/collections/{collection_id}/get_data"
    headers = {"X-API-Key": read_key}
    params = {"limit": limit}

    resp = requests.get(url, headers=headers, params=params, verify=VERIFY_SSL, timeout=60)

    if resp.status_code != 200:
        print(f"❌ Failed fetching data: {resp.status_code} {resp.text}")
        return None

    try:
        data = resp.json()
    except Exception as e:
        print(f"❌ Error parsing JSON response: {e}")
        return None

    # Normalize: if API returns a dict, try to extract the actual records list
    if isinstance(data, dict):
        # Common patterns: {"records": [...]} or {"data": [...]}
        if "records" in data and isinstance(data["records"], list):
            return data["records"]
        if "data" in data and isinstance(data["data"], list):
            return data["data"]

        # Fall back: we don't know which field is right, show raw and abort
        print("⚠ Response JSON is a dict, not a list. Full payload:")
        print(json.dumps(data, indent=2))
        return None

    # If it's already a list, just return it
    if isinstance(data, list):
        return data

    print(f"⚠ Unexpected response type: {type(data)}")
    return None

# ───────────────────────────────────────────────────────────────
# Pretty-print summary of dataset
# ───────────────────────────────────────────────────────────────
def summarize(records):
    if not records:
        print("⚠ No records found in collection.")
        return

    print("\n🔍 Showing up to 24 records:\n")
    limit = min(len(records), 24)

    for rec in records[:limit]:
        print(json.dumps(rec, indent=2))
        print("-" * 40)

    # Timestamp range
    ts_values = [r.get("timestamp") for r in records if "timestamp" in r]
    try:
        ts_parsed = [datetime.fromisoformat(t.replace("Z","")) for t in ts_values]
        print(f"\n⏱ Timestamp range: {min(ts_parsed)} → {max(ts_parsed)}")
    except Exception:
        print("⚠ Could not parse timestamps for range analysis.")

    # Schema inferred from keys
    all_keys = set()
    for r in records:
        all_keys.update(r.keys())

    print("\n🧬 Inferred schema fields:")
    for k in sorted(all_keys):
        print(" -", k)

    print("\n✅ Sanity check complete.\n")


# ───────────────────────────────────────────────────────────────
# Main entrypoint
# ───────────────────────────────────────────────────────────────
def main(station_code: str):
    global BASE_URL  # allow assignment

    settings, secrets = load_config()

    PROJECT_ID = settings["PROJECT_ID"]
    BASE_URL = settings["BASE_URL"].rstrip("/")
    READ_KEY = secrets["read"]

    # 1. Find collection ID
    collection_id = get_collection_id(settings, station_code)
    if not collection_id:
        return

    print(f"ℹ Using collection ID: {collection_id}")

    # 2. Get data from collection
    data = fetch_collection_data(PROJECT_ID, collection_id, READ_KEY)

    # 3. Summarize
    summarize(data)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: sanity_check.py <station_code>")
        sys.exit(1)

    station_code = sys.argv[1]
    main(station_code)