import io
from typing import List, Tuple
from pathlib import Path

import pandas as pd
import requests

def download_csv(url: str, verify_ssl: bool = False, timeout: int = 30) -> pd.DataFrame:
    """
    Download a MeteoSwiss CSV file.

    Args:
        url: URL to the MeteoSwiss CSV file
        verify_ssl: Whether to verify SSL certificates
        timeout: Request timeout in seconds

    Returns:
        DataFrame with CSV content (assumes semicolon delimiter)

    Raises:
        RuntimeError: If download or parsing fails
    """
    try:
        r = requests.get(url, verify=verify_ssl, timeout=timeout)
        r.raise_for_status()
        return pd.read_csv(io.BytesIO(r.content), delimiter=";")
    except requests.RequestException as exc:
        raise RuntimeError(f"Failed to download CSV from {url}: {exc}") from exc
    except pd.errors.ParserError as exc:
        raise RuntimeError(f"Failed to parse CSV content from {url}: {exc}") from exc


def get_station_lat_lon(
    station_code: str,
    station_meta_file: Path | str
) -> Tuple[float | None, float | None]:
    """
    Look up latitude/longitude for a station code from metadata file.

    Args:
        station_code: Station abbreviation (e.g., 'TAE')
        station_meta_file: Path to station metadata CSV file

    Returns:
        Tuple of (latitude, longitude) as floats, or (None, None) if not found

    Raises:
        RuntimeError: If file cannot be read or parsed
    """
    try:
        sc = station_code.upper().strip()
        df_meta = pd.read_csv(station_meta_file, sep=";", encoding="latin1", dtype=str)
        
        if df_meta.empty:
            raise ValueError("Station metadata file is empty")
        
        if "Abbr." not in df_meta.columns:
            raise ValueError("Missing 'Abbr.' column in station metadata")
        
        df_meta["Abbr."] = df_meta["Abbr."].str.strip().str.upper()
        row = df_meta[df_meta["Abbr."] == sc]
        
        if row.empty:
            print(f"⚠️  Station {sc} not found in metadata; returning (None, None)")
            return None, None
        
        if "Latitude" not in row.columns or "Longitude" not in row.columns:
            raise ValueError("Missing 'Latitude' or 'Longitude' columns in station metadata")
        
        lat_str = row["Latitude"].iloc[0]
        lon_str = row["Longitude"].iloc[0]
        
        lat = float(lat_str)
        lon = float(lon_str)
        return lat, lon
        
    except FileNotFoundError as exc:
        raise RuntimeError(f"Station metadata file not found: {station_meta_file}") from exc
    except ValueError as exc:
        print(f"⚠️  Failed to parse lat/lon for station {station_code}: {exc}")
        return None, None
    except pd.errors.ParserError as exc:
        raise RuntimeError(f"Failed to parse station metadata CSV: {exc}") from exc



def rename_biosense(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename MeteoSwiss columns to Biosense/weather schema format.
    
    Maps MeteoSwiss column names to Biosense-style names that match
    the weather_schema structure.

    Args:
        df: Dataframe with MeteoSwiss-columns

    Returns:
        Dataframe with Biosense-style column names

    Raises:
        RuntimeError: If required 'reference_timestamp' column is missing
    """
    try:
        if "reference_timestamp" not in df.columns:
            raise ValueError("Missing required 'reference_timestamp' column")
        
        column_map = {
            "air-temperature_celsius": "tre200h0",
            "air-temperature_celsius_5cm": "tre005h0",
            "air-humidity_percent": "ure200h0",
            "vapor-pressure_hpa": "pva200h0",
            "dew-point_celsius": "tde200h0",
            "air-pressure_hpa": "prestah0",
            "wind-direction_angle": "dkl010h0",
            "wind-speed_m/s": "fkl010h0",
            "gust-peak_m/s": "fkl010h1",
            "percipitation_mm": "rre150h0",
            "snow-depth_cm": "htoauths",
            "solar-radiation_w/m2": "gre000h0",
            "sunshine-duration_w/m2": "sre000h0",
            "reference-evaporation_mm/h": "erefaoh0"
        }
        
        # Only keep columns that are present
        valid_map = {new: old for new, old in column_map.items() if old in df.columns}
        
        cols_to_keep = ["reference_timestamp"] + list(valid_map.values())
        
        # Create proper rename mapping: {old_name: new_name}
        rename_mapping = {old: new for new, old in valid_map.items()}
        
        df_biosense = df[cols_to_keep].rename(columns=rename_mapping)
    
        return df_biosense
        
    except (ValueError, KeyError) as exc:
        raise RuntimeError(f"Biosense renaming failed: {exc}") from exc


def make_records(
    df: pd.DataFrame,
    station_code: str,
    lat: float | None = None,
    lon: float | None = None
) -> List[dict]:
    """
    Convert dataframe into API-ready records for ingestion.

    Each row becomes a record with:
    - key: station code
    - timestamp: reference_timestamp column
    - latitude_4326/longitude_4326: station coordinates (if provided)
    - Numeric columns from dataframe

    Args:
        df: Dataframe with 'reference_timestamp' and numeric data columns
        station_code: Station identifier for 'key' field
        lat: Station latitude (optional)
        lon: Station longitude (optional)

    Returns:
        List of dictionaries ready for API ingestion

    Raises:
        RuntimeError: If required 'reference_timestamp' column is missing
    """
    try:
        if "reference_timestamp" not in df.columns:
            raise ValueError("Missing required 'reference_timestamp' column")
        
        if df.empty:
            print("⚠️  Dataframe is empty, returning empty record list")
            return []
                
        station_key = station_code.upper()
        records: List[dict] = []
        
        for idx, row in df.iterrows():
            raw_ts = row["reference_timestamp"]
            
            try:
                ts = pd.to_datetime(raw_ts).strftime("%Y-%m-%dT%H:%M:%S")
            except Exception as ts_exc:
                print(f"⚠️  Failed to parse timestamp at row {idx}, using raw value: {ts_exc}")
                ts = str(raw_ts)
            
            payload: dict = {}
            
            for c in df.columns:
                if c == "reference_timestamp":
                    continue
                try:
                    fv = float(row[c])
                    if pd.notna(fv):
                            payload[c] = fv
                    else:
                        payload[c] = None
                except (ValueError, TypeError) as e:
                        # Keep the key present even when the value is not numeric.
                        payload[c] = None
            
            if lat is not None:
                payload["latitude_4326"] = float(lat)
            if lon is not None:
                payload["longitude_4326"] = float(lon)
            
            record = {
                "key": station_key,
                "timestamp": ts,
                **payload,
            }
            
            records.append(record)
        
        return records
        
    except ValueError as exc:
        raise RuntimeError(f"Record creation failed: {exc}") from exc
