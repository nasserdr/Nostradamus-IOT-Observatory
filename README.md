# 🌦️ Nostradamus IoT Observatory — Data Ingestion Toolkit
This repository contains the codebase responsible for **ingesting Swiss MeteoSwiss hourly meteorological data into the Nostradamus IoT Observatory**, hosted at the ARISTOTLE UNIVERSITY OF THESSALONIKI. It automates both **historical backfilling** and **daily incremental updates**, ensuring that the Observatory always contains a complete and up-to-date dataset.

---

## 📌 Overview

The ingestion workflow has two components:

### 1️⃣ Historical Backfill
- Fetch all available historical MeteoSwiss *hourly* data.
- Retrieve everything up to **yesterday** or from **from_date (YYYY-MM-DD)** up to **yesterday**.
- Upload the full dataset in bulk to the Observatory.

### 2️⃣ Daily Incremental Updates (Automated)
- A daily **crontab job** fetches the data from yesterday.
- Ensures continuous synchronization with MeteoSwiss.

---

## 🛠️ When You Should Use This Code

Use this toolkit when:

### ✔️ 1. Re-ingesting Data for a Station
Useful for:
- Infrastructure migration  
- Data corruption or loss  
- Failed historical ingestion  
- Data validation campaigns  

### ✔️ 2. Adding a New MeteoSwiss Station
- All stations are referenced via their **MeteoSwiss station codes**.  
- See the list of station codes in the file: config/meteoswiss_stations.csv or at [MeteoSwiss](https://www.meteoswiss.admin.ch/climate/the-climate-of-switzerland/records-and-extremes/extreme-value-analyses/background-information/station-information.html)

---

## 🚀 Quickstart

### 1. Prerequisites
- Python 3.11
- uv (recommended package/environment manager)

### 2. Clone and install dependencies
```bash
uv sync
```

### 3. Configure settings and secrets
Replace secrets.json fields with your keys:
```bash
{
  "master": "your-master-key",
  "read": "your-read-key",
  "write": "your-write-key"
}
```
### 4. Run ingestion
Before ingesting weather data, one has to create a collection for <station_code>
```bash
uv run python ingest_data.py <station_code> --create-collection
```

Recent-only mode (yesterday only):
```bash
uv run python ingest_data.py <station_code>
```

Historical mode:
```bash
uv run python ingest_data.py <station_code> --historical
```

Historical model from a date:
```bash
uv run python ingest_data.py <station_code> --historical --historical-from YYYY-MM-DD
```

## ⏰ Setting Up the Daily Crontab

Create a job that runs every day at 00:01, ingesting the previous day's data:

```bash
1 0 * * * cd /path/to/project && uv run python ingest_data.py <station_code>
```

## 🌡️ Meteorological Variables & Units

| Variable Name        | Description           | Unit   | Example |
|----------------------|-----------------------|--------|---------|
| temperature_c        | Air temperature       | °C     | 12.3    |
| humidity_percent     | Relative humidity     | %      | 76      |
| wind_speed_ms        | Wind speed            | m/s    | 3.1     |
| wind_direction_deg   | Wind direction        | degrees| 240     |
| precip_mm            | Precipitation         | mm     | 0.4     |
| global_rad_wm2       | Global radiation      | W/m²   | 240     |
| pressure_hpa         | Air pressure          | hPa    | 1013    |
| snow_height_cm       | Snow height           | cm     | 12      |


## 📁 Folder Structure Overview
```
project/
├── scripts/
│   ├── get_and_ingest_historical_data_hourly.py
│   ├── ingest_daily_data_hourly.py
│   ├── verify_historical_data_hourly.py
├── config/
│   ├── settings.toml
│   ├── secrets.toml
│   ├── ogd-smn_meta_parameters.csv
│   ├── ch.meteoschweiz.messnetz-automatisch_en.csv
├── logs/
│   ├── some_csv.log
└── README.md
```

## 🔐 Environment Variables & Secrets
Configure the configs.json file as shown below:
```
{
    "PROJECT_ID": <PROJECT_ID>,
    "BASE_URL": <BASE_URL>,
    "master": <MASTER_KEY>,
    "read": <READ_KEY>,
    "write": <WRITE_KEY>
}
```

## 📬 Questions or Contributions?

Open an Issue or contact the ingestion pipeline maintainer.
The pipeline evolves as new stations and sensors are added.
