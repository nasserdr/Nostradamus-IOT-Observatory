#!/usr/bin/env python
import io
import json
import re
import os
from typing import Dict, List, Tuple
from datetime import datetime, timezone

import argparse
import pandas as pd
import requests
import urllib3

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SETTINGS_PATH = "config/settings.json"
SECRETS_PATH = "config/secrets.json"
PARAM_FILE = "config/ogd-smn_meta_parameters.csv"
STATION_META_FILE = "config/ch.meteoschweiz.messnetz-automatisch_en.csv"

with open(SECRETS_PATH, "r", encoding="utf-8") as f:
    secrets = json.load(f)

with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
    cfg = json.load(f)

PROJECT_ID = cfg["PROJECT_ID"]
BASE_URL = cfg["BASE_URL"].rstrip("/")
MASTER_KEY = secrets.get("master")
WRITE_KEY = secrets.get("write")

VERIFY_SSL = False  # set to True when TLS/proxy is properly configured


# ──────────────────────────────────────────────────────────────────────────────
# Download & metadata helpers
# ──────────────────────────────────────────────────────────────────────────────

def download_csv(url: str) -> pd.DataFrame:
    """Download MeteoSwiss CSV (semicolon-separated)."""
    r = requests.get(url, verify=VERIFY_SSL, timeout=30)
    r.raise_for_status()
    return pd.read_csv(io.BytesIO(r.content), delimiter=";")


def build_param_map_from_df(meta_df: pd.DataFrame) -> Dict[str, str]:
    """Use parameter + English description to build map."""
    sub = meta_df.iloc[:, [0, 4]].dropna()
    sub.columns = ["parameter", "parameter_description_en"]
    return dict(
        zip(
            sub["parameter"].str.strip(),
            sub["parameter_description_en"].str.strip()
        )
    )


def clean_name(s: str) -> str:
    """Convert arbitrary description to snake_case."""
    s = s.split(";")[0]
    s = s.lower()
    s = re.sub(r"[^0-9a-zA-Z]+", "_", s)
    return s.strip("_")


def get_station_lat_lon(station_code: str) -> Tuple[float | None, float | None]:
    """
    Look up latitude/longitude for the given station code (Abbr.)
    in STATION_META_FILE and return (lat, lon) as floats.

    Returns (None, None) if not found.
    """
    sc = station_code.upper().strip()
    df_meta = pd.read_csv(STATION_META_FILE, sep=";", encoding="latin1", dtype=str)

    df_meta["Abbr."] = df_meta["Abbr."].str.strip().str.upper()
    row = df_meta[df_meta["Abbr."] == sc]

    if row.empty:
        print(f"⚠ Station {sc} not found in metadata; no lat/long will be added.")
        return None, None

    lat_str = row["Latitude"].iloc[0]
    lon_str = row["Longitude"].iloc[0]

    try:
        lat = float(lat_str)
        lon = float(lon_str)
        return lat, lon
    except Exception:
        print(f"⚠ Failed to parse lat/lon for station {sc} (values: {lat_str}, {lon_str}).")
        return None, None


# ──────────────────────────────────────────────────────────────────────────────
# Column renaming
# ──────────────────────────────────────────────────────────────────────────────

def rename_data_columns_meteoswiss_metadata(
    df_data: pd.DataFrame,
    param_map_full: Dict[str, str]
) -> pd.DataFrame:
    """
    Use MeteoSwiss parameter metadata to rename columns to English descriptions,
    then snake_case.
    """
    present_map = {c: param_map_full[c] for c in df_data.columns if c in param_map_full}
    present_map = {k: clean_name(v) for k, v in present_map.items()}
    return df_data.rename(columns=present_map)


def rename_biosense(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename MeteoSwiss metadata-based columns to target schema names.

    Resulting columns will be:

      reference_timestamp
      air-temperature_celsius
      air-pressure_mbar
      air-humidity_percent
      vapor-pressure_mbar
      wind-speed_m_s
      wind-direction_angle
      wind-gust_m_s
      gust-peak_m_s
      solar-radiation_w_m2
      precipitation_mm
      dew-point_celsius
    """
    column_map = {
        "air-temperature_celsius": "air_temperature_2_m_above_ground",
        "air-pressure_mbar": "atmospheric_pressure_at_barometric_altitude_qfe",
        "air-humidity_percent": "relative_air_humidity_2_m_above_ground",
        "vapor-pressure_mbar": "vapour_pressure_2_m_above_ground",
        "wind-speed_m_s": "wind_speed_scalar",
        "wind-direction_angle": "wind_direction",
        "wind-gust_m_s": "gust_peak_one_second",
        "gust-peak_m_s": "gust_peak_three_seconds",
        "solar-radiation_w_m2": "global_radiation",
        "precipitation_mm": "precipitation",
        "dew-point_celsius": "dew_point_2_m_above_ground",
    }

    valid_map = {new: old for new, old in column_map.items() if old in df.columns}

    cols_to_keep = ["reference_timestamp"] + list(valid_map.values())
    df_biosense = df[cols_to_keep].rename(
        columns={old: new for new, old in valid_map.items()}
    )
    return df_biosense


# ──────────────────────────────────────────────────────────────────────────────
# Records & API helpers
# ──────────────────────────────────────────────────────────────────────────────

def make_records(
    df: pd.DataFrame,
    station_code: str,
    lat: float | None = None,
    lon: float | None = None
) -> List[dict]:
    """
    Convert dataframe into API-ready records.
    - timestamp = reference_timestamp
    - key       = station_code argument (uppercased)
    - latitude_4326 / longitude_4326 are constant per station (if available).
    """
    station_key = station_code.upper()
    records: List[dict] = []

    for _, row in df.iterrows():
        raw_ts = row["reference_timestamp"]

        try:
            ts = pd.to_datetime(raw_ts).strftime("%Y-%m-%dT%H:%M:%S")
        except Exception:
            ts = str(raw_ts)

        payload: dict = {}

        for c in df.columns:
            if c == "reference_timestamp":
                continue
            try:
                fv = float(row[c])
                if pd.notna(fv):
                    payload[c] = fv
            except Exception:
                # non-numerical or missing -> skip
                pass

        if lat is not None:
            payload["latitude_4326"] = float(lat)
        if lon is not None:
            payload["longitude_4326"] = float(lon)

        records.append(
            {
                "key": station_key,
                "timestamp": ts,
                **payload,
            }
        )

    return records


def list_collections(project_id: str, master_key: str) -> List[dict]:
    url = f"{BASE_URL}/projects/{project_id}/collections"
    headers = {"X-API-Key": master_key}

    resp = requests.get(url, headers=headers, verify=VERIFY_SSL, timeout=30)
    if not resp.ok:
        print(f"⚠ Failed to list collections: {resp.status_code} {resp.text}")
        return []

    try:
        return resp.json()
    except Exception as e:
        print(f"⚠ Failed to parse collections JSON: {e}")
        return []


def get_collection_id_by_name(project_id: str, master_key: str, name: str) -> str | None:
    collections = list_collections(project_id, master_key)
    for col in collections:
        col_name = col.get("collection_name") or col.get("name")
        if col_name == name:
            return col.get("collection_id") or col.get("id")
    return None


def create_collection_with_example(
    project_id: str,
    master_key: str,
    name: str,
    description: str,
    tags: List[str],
    example_record: dict,
) -> str | None:
    """
    Create a collection where the schema is inferred from `example_record`.
    Returns the collection_id (or None on failure).
    """
    url = f"{BASE_URL}/projects/{project_id}/collections"
    headers = {"X-API-Key": master_key}

    payload = {
        "name": name,
        "description": description,
        "tags": tags,
        "collection_schema": example_record,
    }

    print(f"➡ Creating collection '{name}' at: {url}")
    resp = requests.post(
        url, json=payload, headers=headers, verify=VERIFY_SSL, timeout=30
    )

    print("Create collection status:", resp.status_code)
    print("Response:", resp.text)

    if resp.ok:
        # The API only returns a message, so we look up the collection ID by name.
        collection_id = get_collection_id_by_name(project_id, master_key, name)
        if collection_id:
            print("✅ Collection created with ID:", collection_id)
            return collection_id
        else:
            print("⚠ Collection created but could not retrieve ID via list endpoint.")
            return None

    # If status not ok, maybe it already exists → try lookup by name anyway
    print(
        "⚠ Could not create collection (maybe it already exists). "
        "Will try to fetch ID by name."
    )
    return get_collection_id_by_name(project_id, master_key, name)


def send_data(project_id: str, collection_id: str, write_key: str, data: List[dict]) -> bool:
    url = f"{BASE_URL}/projects/{project_id}/collections/{collection_id}/send_data"
    headers = {"X-API-Key": write_key}
    try:
        resp = requests.post(
            url, json=data, headers=headers, timeout=60, verify=VERIFY_SSL
        )
    except requests.exceptions.SSLError as e:
        print(f"❌ SSL error while sending data: {e}")
        return False

    if resp.status_code == 200:
        print(f"✅ Sent {len(data)} records")
        return True

    print(f"❌ Failed sending data ({resp.status_code}): {resp.text}")
    return False


# ──────────────────────────────────────────────────────────────────────────────
# Settings.json update helper
# ──────────────────────────────────────────────────────────────────────────────

def save_collection_id_in_settings(station_code: str, collection_id: str):
    """
    Save the collection ID into config/settings.json under key:
      collection_id_<station_code_lower>
    Example: collection_id_tae
    """
    station_code_lower = station_code.lower()
    key_name = f"collection_id_{station_code_lower}"

    # Reload settings to be safe
    with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
        settings = json.load(f)

    settings[key_name] = collection_id

    with open(SETTINGS_PATH, "w", encoding="utf-8") as f:
        json.dump(settings, f, indent=2, ensure_ascii=False)

    print(f"💾 Saved collection ID to {SETTINGS_PATH} as '{key_name}': {collection_id}")


# ──────────────────────────────────────────────────────────────────────────────
# Main logic: create collection + ingest same-day data
# ──────────────────────────────────────────────────────────────────────────────

def main(station_code: str):
    station_code = station_code.lower().strip()

    # Real-time hourly data (NOW) for this station
    url_data = (
        f"https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/"
        f"{station_code}/ogd-smn_{station_code}_h_now.csv"
    )
    print(f"Downloading: {url_data}")

    # 1) Download raw data
    df_data = download_csv(url_data)

    # 2) Parse reference_timestamp
    df_data["reference_timestamp"] = pd.to_datetime(
        df_data["reference_timestamp"], format="%d.%m.%Y %H:%M"
    )

    # Filter for same day in UTC
    today_utc = datetime.now(timezone.utc).date()
    df_today = df_data[df_data["reference_timestamp"].dt.date == today_utc]

    # If nothing for today, fall back to last available day
    if df_today.empty:
        if df_data.empty:
            print(f"⚠ No data at all for station {station_code.upper()} in NOW file.")
            return
        last_date = df_data["reference_timestamp"].dt.date.max()
        print(
            f"⚠ No data for today ({today_utc}), using last available date: {last_date}"
        )
        df_today = df_data[df_data["reference_timestamp"].dt.date == last_date]

    # 3) Load parameter metadata + rename columns
    df_param = pd.read_csv(PARAM_FILE, sep=";", encoding="latin1", dtype=str)
    param_map = build_param_map_from_df(df_param)

    df_named = rename_data_columns_meteoswiss_metadata(df_today, param_map)

    # Optional: save for debugging
    os.makedirs("logs", exist_ok=True)
    df_named.to_csv(
        f"logs/meteoswiss_{station_code}_now_{today_utc}.csv",
        index=False,
    )

    # 4) Biosense / schema naming
    df_biosense = rename_biosense(df_named)

    # 5) Station coordinates
    lat, lon = get_station_lat_lon(station_code)

    # 6) Convert to API records
    records = make_records(df_biosense, station_code, lat=lat, lon=lon)

    if not records:
        print("⚠ No records to send after transformation.")
        return

    # 7) Create or find collection for this station
    coll_name = f"meteoswiss_{station_code}"
    description = (
        f"Weather data from MeteoSwiss {station_code.upper()} station "
        f"(SMN, hourly NOW dataset)."
    )
    tags = ["weather", "meteoswiss", "switzerland"]

    # Try to get existing collection first
    collection_id = get_collection_id_by_name(PROJECT_ID, MASTER_KEY, coll_name)
    if collection_id:
        print(
            f"ℹ Collection '{coll_name}' already exists with ID: {collection_id}"
        )
    else:
        # Create collection using first record as schema example
        example_record = records[0]
        collection_id = create_collection_with_example(
            project_id=PROJECT_ID,
            master_key=MASTER_KEY,
            name=coll_name,
            description=description,
            tags=tags,
            example_record=example_record,
        )
        if not collection_id:
            print("❌ Could not create or find a collection; aborting.")
            return

    # 8) Save collection id into settings.json under collection_id_<station>
    save_collection_id_in_settings(station_code, collection_id)

    # 9) Send all records
    send_data(PROJECT_ID, collection_id, WRITE_KEY, records)


# ──────────────────────────────────────────────────────────────────────────────
# CLI entry point
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Create a Nostradamus collection (if needed) and ingest same-day "
            "MeteoSwiss hourly NOW data for a single SMN station. "
            "Also stores the collection ID in config/settings.json as "
            "collection_id_<station>."
        )
    )
    parser.add_argument(
        "station_code",
        type=str,
        help=(
            "Station code (e.g. tae, aro, ban). "
            "See ./config/ch.meteoschweiz.messnetz-automatisch_en.csv for valid codes."
        ),
    )

    args = parser.parse_args()
    main(args.station_code)
