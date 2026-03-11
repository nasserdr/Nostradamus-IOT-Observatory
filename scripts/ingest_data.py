import os
import argparse
from typing import List
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
import urllib3

import iot_utils
import meteo_utils

STATION_META_PATH = "config/meteoswiss_stations.csv"

DEFAULT_VERIFY_SSL = True
DEFAULT_TIMEOUT = 30
SECRETS_PATH = "config/secrets.json"
SETTINGS_PATH = "config/settings.json"

WEATHER_SCHEMA = {
    'key': 'TAE',
    'timestamp': '2026-11-02T14:00:00',
    'air-temperature_celsius': 9.1,
    'air-temperature_celsius_5cm': 7.9,
    'air-humidity_percent': 72.3,
    'vapor-pressure_hpa': 8.4,
    'dew-point_celsius': 4.4,
    'air-pressure_hpa': 931.5,
    'wind-direction_angle': 229.0,
    'wind-speed_m/s': 4.6,
    'gust-peak_m/s': 10.9,
    'percipitation_mm': 0.2,
    'snow-depth_cm': 0.0,
    'solar-radiation_w/m2': 102.0,
    'sunshine-duration_w/m2': 0.0,
    'reference-evaporation_mm/h': 0.091,
    'latitude_4326': 46.953,
    'longitude_4326': 7.435,
}

def filter_periods_from_date(periods: List[str], from_date: datetime | None) -> List[str]:
    """Return only periods that can contain data on/after from_date.

    Period format is expected to be "YYYY-YYYY". Unknown formats are kept
    to avoid accidentally dropping valid period identifiers.
    """
    if from_date is None:
        return periods

    target_year = from_date.year
    filtered_periods: List[str] = []

    for period in periods:
        parts = period.split("-", maxsplit=1)
        if len(parts) != 2:
            filtered_periods.append(period)
            continue

        try:
            _, end_year = int(parts[0]), int(parts[1])
        except ValueError:
            filtered_periods.append(period)
            continue

        if end_year >= target_year:
            filtered_periods.append(period)

    return filtered_periods

def create_collection(station_code: str,config) -> dict | None:
    """Create collection once, using configured project settings."""
    station_code = station_code.lower().strip()
    return iot_utils.create_collection(
        project_id=config.get("project_id"),
        master_key=config.get("master_key"),
        name="meteoswiss_" + station_code,
        description=(
            f"Weather data from MeteoSwiss {station_code} station"
        ),
        tags=["weather", "meteoswiss", "switzerland"],
        schema=WEATHER_SCHEMA,
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

def _process_and_send_historical(url: str, station_code: str, project_id: str, collection_id: str,
                      write_key: str, base_url: str, lat: float, lon: float,
                      from_date: datetime | None, verify_ssl: bool, timeout: int,
                      label: str = "") -> bool:
    """Download, process, and send data from a given URL."""
    try:
        df = meteo_utils.download_csv(url, verify_ssl=verify_ssl, timeout=timeout)
        df["reference_timestamp"] = pd.to_datetime(df["reference_timestamp"], format="%d.%m.%Y %H:%M")

        # Never ingest today's (or future) records.
        yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
        df = df[df["reference_timestamp"].dt.date <= yesterday]
        if df.empty:
            return False
        
        if from_date is not None:
            df = df[df["reference_timestamp"] >= from_date]
            if df.empty:
                return False
        
        df_biosense = meteo_utils.rename_biosense(df)
        records = meteo_utils.make_records(df_biosense, station_code, lat=lat, lon=lon)
        
        if records:
            print(f"Sending {len(records)} {label} records for {station_code.upper()}")
            iot_utils.send_data(project_id, collection_id, write_key, records, 
                              base_url=base_url, verify_ssl=verify_ssl, timeout=timeout)
            return True
        else:
            print(f"⚠ No {label} records to send for {station_code.upper()}")
            return False
    except Exception as e:
        print(f"⚠ Failed to process {label} data: {e}")
        return False


def ingest_recent_data(config: dict, station_code: str, collection_id: str):
    """Ingest yesterday's hourly data from the MeteoSwiss recent file."""
    station_code = station_code.lower().strip()
    project_id = config.get("project_id", "")
    write_key = config.get("write_key")
    verify_ssl = config.get("verify_ssl", DEFAULT_VERIFY_SSL)
    request_timeout = config.get("request_timeout", DEFAULT_TIMEOUT)
    base_url = config.get("base_url", "")

    url_recent = (
        f"https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/"
        f"{station_code}/ogd-smn_{station_code}_h_recent.csv"
    )
    print(f"Downloading: {url_recent}")

    try:
        df_recent = meteo_utils.download_csv(
            url_recent,
            verify_ssl=verify_ssl,
            timeout=request_timeout,
        )
        df_recent["reference_timestamp"] = pd.to_datetime(
            df_recent["reference_timestamp"],
            format="%d.%m.%Y %H:%M",
        )

        yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
        df_recent = df_recent[df_recent["reference_timestamp"].dt.date == yesterday]
        if df_recent.empty:
            print(f"⚠ No data for {station_code.upper()} on {yesterday}")
            return

        lat, lon = meteo_utils.get_station_lat_lon(station_code, STATION_META_PATH)
        df_biosense = meteo_utils.rename_biosense(df_recent)
        records = meteo_utils.make_records(df_biosense, station_code, lat=lat, lon=lon)

        if records:
            print(f"Sending {len(records)} records for {station_code.upper()} (yesterday)")
            iot_utils.send_data(
                project_id,
                collection_id,
                write_key,
                records,
                base_url=base_url,
                verify_ssl=verify_ssl,
                timeout=request_timeout,
            )
        else:
            print(f"⚠ No records to send for {station_code.upper()} (yesterday)")
    except Exception as e:
        print(f"⚠ Failed to process recent data for {station_code.upper()}: {e}")


def ingest_historical_data(config: dict, station_code: str, from_date: datetime | None = None, collection_id: str = ""):
    """
    Process all available historical hourly files for the given station:
    1980-1989, 1990-1999, 2000-2009, 2010-2019, 2020-2029, and recent data.
    The period 2020-2029 does not contain data from 2026. It is only until last year, so 
    we have to download via the other link
    
    Args:
        station_code: Station code to process
        from_date: Only ingest data from this date onwards (inclusive)
    """
    station_code = station_code.lower().strip()
    project_id = config.get("project_id", "")
    write_key = config.get("write_key")
    verify_ssl = config.get("verify_ssl", DEFAULT_VERIFY_SSL)
    request_timeout = config.get("request_timeout", DEFAULT_TIMEOUT)
    base_url = config.get("base_url", "")
    
    lat, lon = meteo_utils.get_station_lat_lon(station_code, STATION_META_PATH)
    
    historical_periods = ["1980-1989", "1990-1999", "2000-2009", "2010-2019", "2020-2029"]
    periods_to_process = filter_periods_from_date(historical_periods, from_date)

    if not periods_to_process:
        print("No historical periods match the provided --historical-from date")
        return

    # Process only relevant historical periods
    for period in periods_to_process:
        url = f"https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/{station_code}/ogd-smn_{station_code}_h_historical_{period}.csv"
        print(f"Downloading historical {period}: {url}")
        _process_and_send_historical(url, station_code, project_id, collection_id, write_key, 
                         base_url, lat, lon, from_date, verify_ssl, request_timeout, f"({period})")
    
    # Process recent data (current year)
    url_recent = f"https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/{station_code}/ogd-smn_{station_code}_h_recent.csv"
    print(f"Downloading recent data: {url_recent}")
    _process_and_send_historical(url_recent, station_code, project_id, collection_id, write_key, 
                     base_url, lat, lon, from_date, verify_ssl, request_timeout, "(recent)")


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
    config = iot_utils.load_config(secrets_path=SECRETS_PATH, settings_path=SETTINGS_PATH)

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
