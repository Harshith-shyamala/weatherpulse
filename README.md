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
  A["Open-Meteo API (Hourly JSON)"] --> B["Bronze Ingestion (fetch_openmeteo.py)"]
  B --> C["Bronze Parquet (partitioned by dt and location)"]
  B --> M["Bronze Metadata (_meta ingest JSON)"]

  C --> D["Silver Transform (Spark bronze_to_silver.py)"]
  D --> E["Silver Parquet (partitioned by year, month, day, location_part)"]

  E --> F["Gold Aggregations (Spark silver_to_gold.py)"]
  F --> G["Gold Parquet (partitioned by location and date)"]

  G --> H["CSV Exports for Reporting"]
  G --> Q["Data Quality (Great Expectations)"]
