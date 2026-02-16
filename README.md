# WeatherPulse — End-to-End Data Engineering Pipeline (Local)

WeatherPulse is a local, production-style data pipeline that ingests hourly weather data from the Open-Meteo API and builds analytics-ready daily summaries using a **Bronze → Silver → Gold** architecture in **Parquet + Spark**.  
It supports **incremental processing** using **watermarks**, so each run processes only new date partitions.

## Tech Stack
- Python (pandas, pyarrow)
- PySpark (Spark transforms + partitioned data lake)
- YAML config
- Local filesystem (cloud-agnostic design)

---

## Architecture

```mermaid
flowchart LR
  A[Open-Meteo API\nHourly JSON] --> B[Bronze Ingestion\nfetch_openmeteo.py]
  B --> C[(Bronze Parquet\npartitioned by dt/location)]
  B --> M[(Bronze Metadata\n_meta/ingest_<run_id>.json)]

  C --> D[Silver Transform (Spark)\nbronze_to_silver.py]
  D --> E[(Silver Parquet\npartitioned by year/month/day/location_part)]

  E --> F[Gold Aggregations (Spark)\nsilver_to_gold.py]
  F --> G[(Gold Parquet\npartitioned by location/date)]

  G --> H[CSV Exports\nreports/...]
  G -. optional .-> Q[Great Expectations\nData Quality]
