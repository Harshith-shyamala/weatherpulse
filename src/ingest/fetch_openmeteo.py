import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
import yaml
import pandas as pd


BASE_URL = "https://api.open-meteo.com/v1/forecast"
HOURLY_VARS = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "cloud_cover",
    "wind_speed_10m",
]
OUT_BASE = Path("data/bronze/openmeteo_hourly")


def load_locations(config_path: str = "configs/locations.yaml") -> list[dict]:
    here = Path(__file__).resolve()
    project_root = here.parents[2]  # weatherpulse/
    cfg_path = project_root / config_path

    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return cfg["locations"]



def fetch_hourly(lat: float, lon: float, tz: str) -> dict:
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join(HOURLY_VARS),
        "timezone": tz,
        "past_days": 1,
        "forecast_days": 2,
    }
    r = requests.get(BASE_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def to_df(payload: dict, location: str) -> pd.DataFrame:
    hourly = payload.get("hourly", {})
    times = hourly.get("time", [])
    if not times:
        raise ValueError("No hourly.time returned from API")

    df = pd.DataFrame({"event_time": pd.to_datetime(times)})
    for col in HOURLY_VARS:
        df[col] = hourly.get(col)

    df["location"] = location
    df["ingested_at_utc"] = pd.Timestamp.now(tz="UTC")
    df["dt"] = df["event_time"].dt.tz_localize(None).dt.date.astype(str)
    return df


def write_parquet_partitioned(df: pd.DataFrame) -> None:
    for (dt, location), part in df.groupby(["dt", "location"], dropna=False):
        out_dir = OUT_BASE / f"dt={dt}" / f"location={location}"
        out_dir.mkdir(parents=True, exist_ok=True)

        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        out_file = out_dir / f"openmeteo_hourly_{run_id}.parquet"
        part.drop(columns=["dt"]).to_parquet(
            out_file,
            index=False,
            engine="pyarrow",
            coerce_timestamps="us",          # ✅ Spark-friendly timestamp precision
            allow_truncated_timestamps=True  # ✅ ok to drop nanos -> micros
        )



def write_run_metadata(run_meta: dict) -> None:
    meta_dir = Path("data/bronze/_meta")
    meta_dir.mkdir(parents=True, exist_ok=True)
    run_id = run_meta["run_id"]
    with open(meta_dir / f"ingest_{run_id}.json", "w", encoding="utf-8") as f:
        json.dump(run_meta, f, indent=2)


def main():
    start = time.time()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    locations = load_locations()
    parts = []
    errors = []

    for loc in locations:
        name = loc["name"]
        try:
            payload = fetch_hourly(loc["latitude"], loc["longitude"], loc["timezone"])
            df = to_df(payload, location=name)
            parts.append(df)
            print(f"[OK] fetched {name}: {len(df)} rows")
        except Exception as e:
            errors.append({"location": name, "error": str(e)})
            print(f"[ERR] {name}: {e}")

    if not parts:
        raise RuntimeError("No data fetched from any location. Aborting.")

    full = pd.concat(parts, ignore_index=True)
    write_parquet_partitioned(full)

    run_meta = {
        "run_id": run_id,
        "source": "open-meteo",
        "base_url": BASE_URL,
        "hourly_vars": HOURLY_VARS,
        "locations": [l["name"] for l in locations],
        "rows_written": int(len(full)),
        "errors": errors,
        "duration_seconds": round(time.time() - start, 2),
        "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    write_run_metadata(run_meta)

    print(f"\n✅ Bronze ingest complete. rows={len(full)} run_id={run_id}")
    print("Output base:", OUT_BASE.resolve())


if __name__ == "__main__":
    main()
