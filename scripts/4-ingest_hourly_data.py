import io
import json
import re
import os
import argparse
from typing import Dict, List, Tuple
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
import urllib3

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

with open("config/secrets.json", "r", encoding="utf-8") as f:
    secrets = json.load(f)

with open("config/settings.json", "r", encoding="utf-8") as f:
    cfg = json.load(f)

PROJECT_ID = cfg["PROJECT_ID"]
BASE_URL   = cfg["BASE_URL"].rstrip("/")
MASTER_KEY = secrets.get("master")
WRITE_KEY  = secrets.get("write")

# For ingestion we still read the collection ID from settings
COLLECTION_ID       = cfg["COLLECTION_ID"]

PARAM_FILE          = "config/ogd-smn_meta_parameters.csv"
STATION_META_FILE   = "config/ch.meteoschweiz.messnetz-automatisch_en.csv"


# ──────────────────────────────────────────────────────────────────────────────
# Schema for the MeteoSwiss TAE collection  ### NEW
# ──────────────────────────────────────────────────────────────────────────────
weather_schema = {
    "key": "TAE",                   # example station key
    "timestamp": "2025-06-17T10:30:00Z",
    "air-temperature_celsius": 15.2,
    "air-pressure_mbar": 1012.3,
    "air-humidity_percent": 62.5,
    "vapor-pressure_mbar": 12.4,
    "wind-speed_m_s": 3.4,
    "wind-direction_angle": 225,
    "wind-gust_m_s": 5.8,
    "gust-peak_m_s": 7.2,
    "solar-radiation_w_m2": 420.0,
    "precipitation_mm": 0.0,
    "dew-point_celsius": 7.8,
    "latitude_4326": 46.953,
    "longitude_4326": 7.435,
}


# ──────────────────────────────────────────────────────────────────────────────
# Download
# ──────────────────────────────────────────────────────────────────────────────
def download_csv(url: str) -> pd.DataFrame:
    """Download MeteoSwiss CSV (semicolon-separated)."""
    r = requests.get(url, verify=False, timeout=30)
    r.raise_for_status()
    return pd.read_csv(io.BytesIO(r.content), delimiter=";")


def build_param_map_from_df(meta_df: pd.DataFrame) -> Dict[str, str]:
    """Use parameter + English description to build map."""
    sub = meta_df.iloc[:, [0, 4]].dropna()
    sub.columns = ["parameter", "parameter_description_en"]
    return dict(zip(sub["parameter"].str.strip(), sub["parameter_description_en"].str.strip()))


def clean_name(s: str) -> str:
    """Convert arbitrary description to snake_case."""
    s = s.split(";")[0]
    s = s.lower()
    s = re.sub(r"[^0-9a-zA-Z]+", "_", s)
    return s.strip("_")


# ──────────────────────────────────────────────────────────────────────────────
# Station metadata: latitude / longitude
# ──────────────────────────────────────────────────────────────────────────────
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
# Transform (rename columns using MeteoSwiss metadata)
# ──────────────────────────────────────────────────────────────────────────────
def rename_data_columns_meteoswiss_metadata(df_data: pd.DataFrame,
                                            param_map_full: Dict[str, str]) -> pd.DataFrame:
    present_map = {c: param_map_full[c] for c in df_data.columns if c in param_map_full}
    present_map = {k: clean_name(v) for k, v in present_map.items()}
    return df_data.rename(columns=present_map)


# ──────────────────────────────────────────────────────────────────────────────
# Transform (Biosense renaming)
# ──────────────────────────────────────────────────────────────────────────────
def rename_biosense(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename MeteoSwiss columns to the Biosense-style / weather_schema names.
    Resulting columns will match weather_schema keys (except key/timestamp/lat/lon).
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

    # only keep columns that are present
    valid_map = {new: old for new, old in column_map.items() if old in df.columns}

    cols_to_keep = ["reference_timestamp"] + list(valid_map.values())
    df_biosense = df[cols_to_keep].rename(columns={old: new for new, old in valid_map.items()})
    return df_biosense


# ──────────────────────────────────────────────────────────────────────────────
# API record building
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
    - key       = station_code argument
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


# ──────────────────────────────────────────────────────────────────────────────
# Collection creation (using weather_schema)  ### CHANGED
# ──────────────────────────────────────────────────────────────────────────────
def create_collection(
    project_id: str,
    master_key: str,
    name: str,
    description: str,
    tags: List[str],
    schema: dict,
):
    """
    Create a collection with a fixed schema example (no inference from example record).
    """
    url = f"{BASE_URL}/projects/{project_id}/collections"
    headers = {"X-API-Key": master_key}

    payload = {
        "name": name,
        "description": description,
        "tags": tags,
        "collection_schema": schema,
    }

    r = requests.post(url, json=payload, headers=headers, verify=False, timeout=30)
    print("Create collection:", r.status_code, r.text)
    if r.ok:
        data = r.json()
        # Note: check the exact field name used by your API for the ID
        print("Collection ID:", data.get("collection_id") or data.get("id"))
        return data
    return None


def create_tae_collection_once():
    """
    Helper you can run once (manually) to create the TAE collection
    according to weather_schema.
    """
    return create_collection(
        project_id=PROJECT_ID,
        master_key=MASTER_KEY,
        name="meteoswiss_tae",
        description=(
            "Weather data from MeteoSwiss TAE (Tanikon) station, "
            "50 meters away from the Swiss Future Farm (The Swiss Pilot Site for Nostradamus)"
        ),
        tags=["weather", "meteoswiss", "switzerland"],
        schema=weather_schema,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Send data
# ──────────────────────────────────────────────────────────────────────────────
def send_data(project_id: str, collection_id: str, write_key: str, data: List[dict]) -> bool:
    url = f"{BASE_URL}/projects/{project_id}/collections/{collection_id}/send_data"
    headers = {"X-API-Key": write_key}
    try:
        resp = requests.post(url, json=data, headers=headers, timeout=60, verify=False)
    except requests.exceptions.SSLError as e:
        print(f"❌ SSL error while sending data: {e}")
        return False

    if resp.status_code == 200:
        print(f"✅ Sent {len(data)} records")
        return True

    print(f"❌ Failed sending data ({resp.status_code}): {resp.text}")
    return False


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
def main(station_code: str):
    station_code = station_code.lower().strip()

    url_data = (
        f"https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/"
        f"{station_code}/ogd-smn_{station_code}_t_recent.csv"
    )

    print(f"Downloading: {url_data}")

    df_data = download_csv(url_data)

    df_data["reference_timestamp"] = pd.to_datetime(
        df_data["reference_timestamp"], format="%d.%m.%Y %H:%M"
    )

    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    df_yesterday = df_data[df_data["reference_timestamp"].dt.date == yesterday]

    if df_yesterday.empty:
        print(f"⚠ No data for {station_code.upper()} on {yesterday}")
        return

    df_param = pd.read_csv(PARAM_FILE, sep=";", encoding="latin1", dtype=str)
    param_map = build_param_map_from_df(df_param)

    df_named = rename_data_columns_meteoswiss_metadata(df_yesterday, param_map)

    os.makedirs("logs", exist_ok=True)
    df_named.to_csv(f"logs/meteoswiss_{station_code}_{yesterday}.csv", index=False)

    df_biosense = rename_biosense(df_named)

    lat, lon = get_station_lat_lon(station_code)

    records = make_records(df_biosense, station_code, lat=lat, lon=lon)

    if not records:
        print("No records to send.")
        return

    send_data(PROJECT_ID, COLLECTION_ID, WRITE_KEY, records)


def process_historical_station(station_code: str):
    """
    Process all available historical hourly files for the given station:
    1980-1989, 1990-1999, 2000-2009, 2010-2019, 2020-2029.
    """
    station_code = station_code.lower().strip()
    periods = [
        "1980-1989",
        "1990-1999",
        "2000-2009",
        "2010-2019",
        "2020-2029",
    ]

    df_param = pd.read_csv(PARAM_FILE, sep=";", encoding="latin1", dtype=str)
    param_map = build_param_map_from_df(df_param)

    lat, lon = get_station_lat_lon(station_code)

    for period in periods:
        url_data = (
            f"https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/"
            f"{station_code}/ogd-smn_{station_code}_h_historical_{period}.csv"
        )

        print(f"Downloading historical {period}: {url_data}")

        try:
            df_data = download_csv(url_data)
        except Exception as e:
            print(f"⚠ Failed to download {url_data}: {e}")
            continue

        try:
            df_data["reference_timestamp"] = pd.to_datetime(
                df_data["reference_timestamp"], format="%d.%m.%Y %H:%M"
            )
        except Exception as e:
            print(f"⚠ Failed to parse timestamps for {period}: {e}")
            continue

        df_named = rename_data_columns_meteoswiss_metadata(df_data, param_map)

        os.makedirs("logs", exist_ok=True)
        df_named.to_csv(
            f"logs/meteoswiss_{station_code}_historical_{period}.csv",
            index=False,
        )

        df_biosense = rename_biosense(df_named)

        records = make_records(df_biosense, station_code, lat=lat, lon=lon)

        if not records:
            print(f"⚠ No records to send for {station_code.upper()} {period}")
            continue

        print(f"Sending {len(records)} records for {station_code.upper()} {period}")
        send_data(PROJECT_ID, COLLECTION_ID, WRITE_KEY, records)


# ──────────────────────────────────────────────────────────────────────────────
# CLI entry point
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download and push MeteoSwiss data for a single SMN station."
    )
    parser.add_argument(
        "station_code",
        type=str,
        help=(
            "Station code (e.g. tae, aro, rag, ban). "
            "Please check ./config/ch.meteoschweiz.messnetz-automatisch_en.csv for valid codes."
        ),
    )
    parser.add_argument(
        "--historical",
        action="store_true",
        help="If set, process historical hourly data (1980-2029) instead of only yesterday's recent file.",
    )
    parser.add_argument(
        "--create-tae-collection",
        action="store_true",
        help="Create the meteoswiss_tae collection based on weather_schema and exit."
    )

    args = parser.parse_args()

    if args.create_tae_collection:
        create_tae_collection_once()
    elif args.historical:
        process_historical_station(args.station_code)
    else:
        main(args.station_code)
