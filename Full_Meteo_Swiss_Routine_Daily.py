import io
import json
import re
from datetime import datetime
from typing import Dict, List

import pandas as pd
import requests
import urllib3

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

with open("configs.json", "r", encoding="utf-8") as f:
    cfg = json.load(f)

PROJECT_ID = cfg["PROJECT_ID"]
BASE_URL   = cfg["BASE_URL"].rstrip("/")
MASTER_KEY = cfg.get("master")  # only needed if you want to create the collection
WRITE_KEY  = cfg.get("write")   # ← use 'write' for sending data
READ_KEY   = cfg.get("read")    # not used here

# Your target collection id (create it once in your system; keep the id here)
COLLECTION_ID = "Your_collection_id"

# MeteoSwiss source files
URL_DATA = "https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/tae/ogd-smn_tae_t_now.csv"
PARAM_FILE = "ogd-smn_meta_parameters.csv"  # local copy; use latin1

# ──────────────────────────────────────────────────────────────────────────────
# Download + Mapping
# ──────────────────────────────────────────────────────────────────────────────
def download_csv(url: str) -> pd.DataFrame:
    """Download CSV, skip SSL verification, return DataFrame (semicolon-separated)."""
    r = requests.get(url, verify=False, timeout=30)
    r.raise_for_status()
    return pd.read_csv(io.BytesIO(r.content), delimiter=";")

def build_param_map_from_df(meta_df: pd.DataFrame) -> Dict[str, str]:
    """Use column 1 (parameter) and column 5 (parameter_description_en) to build map."""
    sub = meta_df.iloc[:, [0, 4]].dropna()
    sub.columns = ["parameter", "parameter_description_en"]
    return dict(zip(sub["parameter"].str.strip(), sub["parameter_description_en"].str.strip()))

def clean_name(s: str) -> str:
    """Keep before first ';', lowercase, non-alnum→'_', strip '_'."""
    s = s.split(";")[0]
    s = s.lower()
    s = re.sub(r"[^0-9a-zA-Z]+", "_", s)
    return s.strip("_")

# ──────────────────────────────────────────────────────────────────────────────
# Transform (rename to readable columns)
# ──────────────────────────────────────────────────────────────────────────────
def rename_data_columns(df_data: pd.DataFrame, param_map_full: Dict[str, str]) -> pd.DataFrame:
    # restrict mapping to columns present in data
    present_map = {c: param_map_full[c] for c in df_data.columns if c in param_map_full}
    # clean names
    present_map = {k: clean_name(v) for k, v in present_map.items()}
    # apply renaming
    return df_data.rename(columns=present_map)

# ──────────────────────────────────────────────────────────────────────────────
# Shaping to your API schema
# ──────────────────────────────────────────────────────────────────────────────
def detect_columns(df: pd.DataFrame):
    """
    Heuristically find time and station columns common in SMN files.
    Adjust this if your file uses different headers.
    """
    time_candidates = ["time", "date", "timestamp", "mes_ts_utc", "datetime"]
    station_candidates = ["station", "stn", "nat_abbr", "stationcode", "smn_id"]

    time_col = next((c for c in time_candidates if c in df.columns), None)
    stn_col  = next((c for c in station_candidates if c in df.columns), None)

    if time_col is None:
        raise ValueError("Could not find a time column. Please set time_col explicitly.")
    if stn_col is None:
        # If there is no per-station column (single-station file), fallback to a constant key
        stn_col = None

    return time_col, stn_col


def make_records(df: pd.DataFrame) -> List[dict]:
    """
    Turn each row into a record with:
      key: station id (or a constant if not available)
      timestamp: ISO string (no timezone suffix assumed to be UTC)
      metrics: remaining numeric columns in the row
    """
    time_col, stn_col = detect_columns(df)

    # Columns we don't want inside metrics:
    exclude = {time_col}
    if stn_col:
        exclude.add(stn_col)

    # Choose a default key if no station column
    DEFAULT_KEY = "SMN_UNSPECIFIED"

    records = []
    for _, row in df.iterrows():
        # timestamp
        ts_val = row[time_col]
        # try to standardize timestamp
        try:
            ts = pd.to_datetime(ts_val, utc=False).strftime("%Y-%m-%dT%H:%M:%S")
        except Exception:
            ts = str(ts_val)

        # key
        key = str(row[stn_col]) if stn_col else DEFAULT_KEY

        # metrics: numeric-like columns except excluded
        payload = {}
        for c in df.columns:
            if c in exclude:
                continue
            val = row[c]
            # keep only finite numeric or leave as None if not parseable
            if pd.api.types.is_number(val):
                payload[c] = float(val)
            else:
                # try parse to float
                try:
                    fv = float(val)
                    if pd.notna(fv):
                        payload[c] = fv
                except Exception:
                    # drop non-numeric values silently
                    pass

        record = {"key": key, "timestamp": ts}
        record.update(payload)
        records.append(record)

    return records

# ──────────────────────────────────────────────────────────────────────────────
# Upload (time-series)
# ──────────────────────────────────────────────────────────────────────────────
def send_data(project_id: str, collection_id: str, write_key: str, data: List[dict]) -> bool:
    url = f"{BASE_URL}/projects/{project_id}/collections/{collection_id}/send_data"
    headers = {"X-API-Key": write_key}
    resp = requests.post(url, json=data, headers=headers, timeout=60)
    if resp.status_code == 200:
        print(f"✅ Sent {len(data)} records")
        return True
    print(f"❌ Failed to send data ({resp.status_code}): {resp.text}")
    return False

# Optional: if you actually need to create the collection programmatically once.
def create_collection(project_id: str, master_key: str, example_record: dict):
    url = f"{BASE_URL}/projects/{project_id}/collections"
    headers = {"X-API-Key": master_key}
    schema_example = {k: example_record[k] for k in list(example_record.keys())[:15]}  # trim if huge
    payload = {
        "name": "meteoswiss_tenmin",
        "description": "MeteoSwiss SMN ten-minute data",
        "tags": ["meteoswiss", "weather", "smn", "tenmin"],
        "collection_schema": schema_example
    }
    r = requests.post(url, json=payload, headers=headers, verify=False, timeout=30)
    print("Create collection:", r.status_code, r.text)
    return r.json() if r.ok else None

# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
def main():
    # 1) Download data
    df_data = download_csv(URL_DATA)

    # 2) Load parameter dictionary (latin1) and build full map
    df_meta = pd.read_csv(PARAM_FILE, sep=";", encoding="latin1", dtype=str)
    param_map = build_param_map_from_df(df_meta)

    # 3) Rename columns to clean snake_case English
    df_named = rename_data_columns(df_data, param_map)

    # 4) Build records for the API
    records = make_records(df_named, time_col = 'reference_timestamp')
    if not records:
        print("No records to send (after filtering).")
        return

    # 5) (Optional) Create collection once (commented out after first run)
    # create_collection(PROJECT_ID, MASTER_KEY, records[0])

    # 6) Send data
    send_data(PROJECT_ID, COLLECTION_ID, WRITE_KEY, records)

if __name__ == "__main__":
    main()
