# 🌦️ Nostradamus IoT Observatory — Data Ingestion Toolkit
This repository contains the codebase responsible for **ingesting Swiss MeteoSwiss hourly meteorological data into the Nostradamus IoT Observatory**, hosted at the ARISTOTLE UNIVERSITY OF THESSALONIKI. It automates both **historical backfilling** and **daily incremental updates**, ensuring that the Observatory always contains a complete and up-to-date dataset.

---

## 📌 Overview

The ingestion workflow has two components:

### 1️⃣ Historical Backfill (One-Time Per Station)
- Fetch all available historical MeteoSwiss *hourly* data.
- Retrieve everything up to **yesterday**.
- Upload the full dataset in bulk to the Observatory.

### 2️⃣ Daily Incremental Updates (Automated)
- A daily **crontab job** fetches the last 24 hours of data.
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
- **TODO:** Add station list link →  
  https://www.meteoswiss.admin.ch/services-and-publications/portals-and-tools/station-finder.html

---

## 🚀 How to Run the Historical Backfill

### 1. Install Dependencies
```bash
uv sync
```

### 2. Ingest Historical Data
uv run get_and_ingest_historical_data_hourly.py --station <station_code>

### 3. Verify Ingestion
uv run verify_historical_data_hourly.py --station <station_code>

## ⏰ Setting Up the Daily Crontab

Create a job that runs every day at 00:01, ingesting the previous day's data:

1 0 * * * cd /path/to/project && uv run ingest_daily_data_hourly.py --station <station_code> >> /var/log/nostradamus_ingest.log 2>&1

Example for Multiple Stations
1 0 * * * uv run ingest_daily_data_hourly.py --station STATION1
3 0 * * * uv run ingest_daily_data_hourly.py --station STATION2
5 0 * * * uv run ingest_daily_data_hourly.py --station STATION3

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
project/
├── scripts/
│   ├── get_and_ingest_historical_data_hourly.py
│   ├── ingest_daily_data_hourly.py
│   ├── verify_historical_data_hourly.py
├── config/
│   ├── settings.toml
│   ├── secrets.toml
├── modules/
│   ├── ingestion.py
│   ├── meteo_fetcher.py
│   ├── utils.py
├── logs/
│   ├── ingestion.log
│   ├── verification.log
└── README.md

## 🔐 Environment Variables & Secrets
Configure the configs.json file as shown below:
```
{
    "PROJECT_ID": "161854af-716b-49e8-bc55-cd975699db54",
    "BASE_URL": "https://nostradamus-ioto.issel.ee.auth.gr/api/v1",
    "master": "4c2e33648ea207d68e2421f5bf17849900abb2b9f540eeffb56b3514edd5fd56",
    "read": "7cd6fa2146c27733efba64d084b5da6f8c10de562a52fdacecd6a07be532e00f",
    "write": "74b5b8d39b519a4b40d8f683028a5e400193ae0cfe6c51a1cd266e177283d840"
}
```

## 📬 Questions or Contributions?

Open an Issue or contact the ingestion pipeline maintainer.
The pipeline evolves as new stations and sensors are added.
