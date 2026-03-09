import io
import re
from typing import Any, Dict, List, Tuple
from pathlib import Path

import pandas as pd
import requests
import urllib3

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


def build_param_map_from_df(meta_df: pd.DataFrame) -> Dict[str, str]:
    """
    Build a parameter mapping from MeteoSwiss metadata dataframe.

    Extracts parameter names (column 0) and English descriptions (column 4),
    creating a dictionary for column renaming.

    Args:
        meta_df: Metadata dataframe from parameters CSV

    Returns:
        Dictionary mapping parameter codes to English descriptions
    """
    sub = meta_df.iloc[:, [0, 4]].dropna()
    sub.columns = ["parameter", "parameter_description_en"]
    return dict(
        zip(
            sub["parameter"].str.strip(),
            sub["parameter_description_en"].str.strip()
        )
    )


def clean_name(s: str) -> str:
    """
    Convert arbitrary description string to snake_case.

    Handles semicolon-separated values, converts to lowercase,
    and replaces non-alphanumeric characters with underscores.

    Args:
        s: String to clean (e.g., "Air Temperature (°C);alternate")

    Returns:
        Cleaned snake_case string (e.g., "air_temperature_c")
    """
    s = s.split(";")[0]           # Take first part if semicolon-separated
    s = s.lower()                  # Convert to lowercase
    s = re.sub(r"[^0-9a-zA-Z]+", "_", s)  # Replace non-alphanumeric with underscore
    return s.strip("_")            # Remove leading/trailing underscores


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


def rename_data_columns_meteoswiss_metadata(
    df_data: pd.DataFrame,
    param_map: Dict[str, str]
) -> pd.DataFrame:
    """
    Rename dataframe columns using MeteoSwiss parameter mapping.

    Args:
        df_data: Raw MeteoSwiss data dataframe
        param_map: Parameter mapping dictionary from build_param_map_from_df()

    Returns:
        Dataframe with renamed columns in snake_case

    Raises:
        RuntimeError: If dataframe is empty or has unexpected structure
    """
    try:
        if df_data.empty:
            raise ValueError("Data dataframe is empty")
        
        present_map = {c: param_map[c] for c in df_data.columns if c in param_map}
        if not present_map:
            raise ValueError("No columns matched parameter mapping")
        
        present_map = {k: clean_name(v) for k, v in present_map.items()}
        return df_data.rename(columns=present_map)
        
    except ValueError as exc:
        raise RuntimeError(f"Column renaming failed: {exc}") from exc


def rename_biosense(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename MeteoSwiss columns to Biosense/weather schema format.
    
    Maps MeteoSwiss column names to Biosense-style names that match
    the weather_schema structure.

    Args:
        df: Dataframe with MeteoSwiss-renamed columns

    Returns:
        Dataframe with Biosense-style column names

    Raises:
        RuntimeError: If required 'reference_timestamp' column is missing
    """
    try:
        if "reference_timestamp" not in df.columns:
            raise ValueError("Missing required 'reference_timestamp' column")
        
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
        
        # Only keep columns that are present
        valid_map = {new: old for new, old in column_map.items() if old in df.columns}
        
        cols_to_keep = ["reference_timestamp"] + list(valid_map.values())
        df_biosense = df[cols_to_keep].rename(columns={old: new for new, old in valid_map.items()})
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
                except (ValueError, TypeError):
                    # Non-numerical or missing -> skip
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
        
    except ValueError as exc:
        raise RuntimeError(f"Record creation failed: {exc}") from exc
