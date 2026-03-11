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

| Variable Name | Description | Unit | Example |
|---|---|---|---|
| air-temperature_celsius | Air temperature at 2 m | °C | 9.1 |
| air-temperature_celsius_5cm | Air temperature at 5 cm | °C | 7.9 |
| air-humidity_percent | Relative humidity | % | 72.3 |
| vapor-pressure_hpa | Vapor pressure | hPa | 8.4 |
| dew-point_celsius | Dew point temperature | °C | 4.4 |
| air-pressure_hpa | Air pressure | hPa | 931.5 |
| wind-direction_angle | Wind direction | degrees | 229.0 |
| wind-speed_m/s | Wind speed | m/s | 4.6 |
| gust-peak_m/s | Gust peak speed | m/s | 10.9 |
| percipitation_mm | Precipitation | mm | 0.2 |
| snow-depth_cm | Snow depth | cm | 0.0 |
| solar-radiation_w/m2 | Solar radiation | W/m² | 102.0 |
| sunshine-duration_w/m2 | Sunshine duration | project-specific convention | 0.0 |
| reference-evaporation_mm/h | Reference evaporation | mm/h | 0.091 |
| latitude_4326 | Latitude (WGS84 / EPSG:4326) | decimal degrees | 46.953 |
| longitude_4326 | Longitude (WGS84 / EPSG:4326) | decimal degrees | 7.435 |


## 📁 Folder Structure Overview
```
Nostradamus-IOT-Observatory/
├── .gitignore
├── pyproject.toml
├── uv.lock
├── README.md
├── config/
│   ├── meta_parameters.csv
│   ├── meteoswiss_stations.csv
│   ├── secrets.json
│   └── settings.json
└── scripts/
  ├── ingest_data.py
  ├── iot_utils.py
  └── meteo_utils.py
```

## 📬 Questions or Contributions?

Open an Issue or contact the ingestion pipeline maintainer.
The pipeline evolves as new stations and sensors are added.
