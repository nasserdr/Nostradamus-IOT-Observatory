import os
import argparse
from typing import List
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
import urllib3

import iot_utils
import meteo_utils

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def _runtime_config() -> dict:
    """Load runtime configuration on demand."""
    return iot_utils.load_config()

PARAM_FILE = "config/ogd-smn_meta_parameters.csv"
STATION_META_FILE = "config/ch.meteoschweiz.messnetz-automatisch_en.csv"

DEFAULT_VERIFY_SSL = True
DEFAULT_TIMEOUT = 30

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


def create_collection(station_code: str,config) -> dict | None:
    """Create collection once, using configured project settings."""
    station_code = station_code.lower().strip()
    return iot_utils.create_collection(
        project_id=config.get("project_id", ""),
        master_key=config.get("master_key"),
        name=station_code,
        description=(
            f"Weather data from MeteoSwiss {station_code} station"
        ),
        tags=["weather", "meteoswiss", "switzerland"],
        schema=weather_schema,
        base_url=config.get("base_url"),
        verify_ssl=config.get("verify_ssl", DEFAULT_VERIFY_SSL),
        timeout=config.get("request_timeout", DEFAULT_TIMEOUT),
    )


def get_collection_id_from_station_code(config: dict, station_code: str) -> str:
    """Resolve collection id for a station using loaded config values."""

    return iot_utils.get_collection_id_from_station_code(
        station_code=station_code,
        project_id=config.get("project_id"),
        api_key=config.get("read_key"),
        base_url=config.get("base_url"),
        timeout=config.get("request_timeout", DEFAULT_TIMEOUT),
        verify_ssl=config.get("verify_ssl", DEFAULT_VERIFY_SSL),
    )


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
def ingest_recent_data(config: dict, station_code: str, collection_id: str):

    project_id = config.get("project_id")
    write_key = config.get("write_key")
    base_url = config.get("base_url")
    verify_ssl = config.get("verify_ssl", DEFAULT_VERIFY_SSL)
    request_timeout = config.get("request_timeout", DEFAULT_TIMEOUT)

    station_code = station_code.lower().strip()

    url_data = (
        f"https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/"
        f"{station_code}/ogd-smn_{station_code}_t_recent.csv"
    )

    print(f"Downloading: {url_data}")

    df_data = meteo_utils.download_csv(url_data, verify_ssl=verify_ssl, timeout=request_timeout)

    df_data["reference_timestamp"] = pd.to_datetime(
        df_data["reference_timestamp"], format="%d.%m.%Y %H:%M"
    )

    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    df_yesterday = df_data[df_data["reference_timestamp"].dt.date == yesterday]

    if df_yesterday.empty:
        print(f"⚠ No data for {station_code.upper()} on {yesterday}")
        return

    df_param = pd.read_csv(PARAM_FILE, sep=";", encoding="latin1", dtype=str)
    param_map = meteo_utils.build_param_map_from_df(df_param)

    df_named = meteo_utils.rename_data_columns_meteoswiss_metadata(df_yesterday, param_map)

    os.makedirs("logs", exist_ok=True)
    df_named.to_csv(f"logs/meteoswiss_{station_code}_{yesterday}.csv", index=False)

    df_biosense = meteo_utils.rename_biosense(df_named)

    lat, lon = meteo_utils.get_station_lat_lon(station_code, STATION_META_FILE)

    records = meteo_utils.make_records(df_biosense, station_code, lat=lat, lon=lon)

    if not records:
        print("No records to send.")
        return

    iot_utils.send_data(project_id, collection_id, write_key, records, base_url=base_url)


def ingest_historical_data(config: dict, station_code: str, from_date: datetime | None = None, collection_id: str = ""):
    """
    Process all available historical hourly files for the given station:
    1980-1989, 1990-1999, 2000-2009, 2010-2019, 2020-2029.
    
    Args:
        station_code: Station code to process
        from_date: Only ingest data from this date onwards (inclusive)
    """
    station_code = station_code.lower().strip()
    project_id = config.get("project_id", "")
    write_key = config.get("write_key")
    verify_ssl = config.get("verify_ssl", DEFAULT_VERIFY_SSL)
    request_timeout = config.get("request_timeout", DEFAULT_TIMEOUT)

    if not project_id or not write_key or not collection_id:
        print("Missing required config (project_id/write_key/collection_id_tae)")
        return

    periods = [
        "1980-1989",
        "1990-1999",
        "2000-2009",
        "2010-2019",
        "2020-2029",
    ]

    base_url = config.get("base_url", "")

    df_param = pd.read_csv(PARAM_FILE, sep=";", encoding="latin1", dtype=str)
    param_map = meteo_utils.build_param_map_from_df(df_param)

    lat, lon = meteo_utils.get_station_lat_lon(station_code, STATION_META_FILE)

    for period in periods:
        url_data = (
            f"https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/"
            f"{station_code}/ogd-smn_{station_code}_h_historical_{period}.csv"
        )

        print(f"Downloading historical {period}: {url_data}")

        try:
            df_data = meteo_utils.download_csv(url_data, verify_ssl=verify_ssl, timeout=request_timeout)
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

        # Filter data based on from_date if specified
        if from_date is not None:
            df_data = df_data[df_data["reference_timestamp"] >= from_date]
            if df_data.empty:
                print(f"⚠ No data in {period} after {from_date.date()}")
                continue

        df_named = meteo_utils.rename_data_columns_meteoswiss_metadata(df_data, param_map)

        os.makedirs("logs", exist_ok=True)
        df_named.to_csv(
            f"logs/meteoswiss_{station_code}_historical_{period}.csv",
            index=False,
        )

        df_biosense = meteo_utils.rename_biosense(df_named)

        records = meteo_utils.make_records(df_biosense, station_code, lat=lat, lon=lon)

        if not records:
            print(f"⚠ No records to send for {station_code.upper()} {period}")
            continue

        print(f"Sending {len(records)} records for {station_code.upper()} {period}")
        iot_utils.send_data(project_id, collection_id, write_key, records, base_url=base_url)


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
        "--historical-from",
        type=str,
        help="Start date for historical data ingestion (format: YYYY-MM-DD). Only used with --historical.",
    )
    parser.add_argument(
        "--create-collection",
        action="store_true",
        help="Create a MeteoSwiss collection for the station based on weather_schema and exit."
    )

    args = parser.parse_args()

    # Load config once
    config = iot_utils.load_config(secrets_path="config/secrets.json", settings_path="config/settings.json")

    if args.create_collection:
        create_collection(args.station_code, config)
    else:
        collection_id = get_collection_id_from_station_code(config, args.station_code)
        if args.historical:
            # Parse from_date if provided
            from_date = None
            if args.historical_from:
                try:
                    from_date = datetime.strptime(args.historical_from, "%Y-%m-%d")
                    print(f"📅 Ingesting historical data from {from_date.date()} onwards")
                except ValueError:
                    print(f"❌ Invalid date format: {args.historical_from}. Expected YYYY-MM-DD")
                    exit(1)
            
            ingest_historical_data(config, args.station_code, from_date, collection_id)
        else:
            ingest_recent_data(config, args.station_code, collection_id)
