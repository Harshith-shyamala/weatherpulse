from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import SparkSession
import builtins
from pyspark.sql.functions import (
    col, to_date, avg,
    min as smin,
    max as smax,
    sum as _sum,
    count, current_timestamp
)



def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


STATE_FILE = "data/_state/gold_watermark.json"


def read_watermark(root: Path) -> str | None:
    p = root / STATE_FILE
    if not p.exists():
        return None
    obj = json.loads(p.read_text(encoding="utf-8"))
    return obj.get("last_date")


def write_watermark(root: Path, last_date: str) -> None:
    p = root / STATE_FILE
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"last_date": last_date}, indent=2), encoding="utf-8")


def list_silver_days(root: Path) -> list[str]:
    # silver partition folders like: year=2026/month=2/day=16
    base = root / "data/silver/openmeteo_hourly"
    if not base.exists():
        return []

    days = []
    for y in base.glob("year=*"):
        for m in y.glob("month=*"):
            for d in m.glob("day=*"):
                if d.is_dir():
                    year = y.name.split("year=")[-1]
                    month = m.name.split("month=")[-1].zfill(2)
                    day = d.name.split("day=")[-1].zfill(2)
                    days.append(f"{year}-{month}-{day}")
    return sorted(set(days))


def main():
    root = project_root()
    silver_path = str(root / "data/silver/openmeteo_hourly")
    gold_path = str(root / "data/gold/weather_daily_summary")  # same folder name, new partitioning scheme

    spark = SparkSession.builder.appName("weatherpulse-silver-to-gold-incremental").getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    last_date = read_watermark(root)
    all_days = list_silver_days(root)

    if not all_days:
        raise RuntimeError("No Silver day partitions found.")

    if last_date is None:
        days_to_process = all_days
    else:
        days_to_process = [d for d in all_days if d > last_date]

    if not days_to_process:
        print(f"✅ No new Silver days. Watermark={last_date}. Nothing to do.")
        spark.stop()
        return

    # Read only the partitions we need by filtering after load (fast enough for our scale)
    df = spark.read.parquet(silver_path).withColumn("date", to_date(col("event_time")))

    daily = (
        df.filter(col("date").cast("string").isin(days_to_process))
          .groupBy("location", "date")
          .agg(
              count("*").alias("hourly_records"),
              avg("temperature_2m").alias("avg_temp_c"),
              smin("temperature_2m").alias("min_temp_c"),
              smax("temperature_2m").alias("max_temp_c"),
              _sum("precipitation").alias("total_precip_mm"),
              avg("wind_speed_10m").alias("avg_wind_kmh"),
              avg("cloud_cover").alias("avg_cloud_cover"),
          )
          .withColumn("gold_generated_at", current_timestamp())
    )

    # ✅ partition by location + date for incremental overwrite
    (
        daily.write
        .mode("overwrite")
        .partitionBy("location", "date")
        .parquet(gold_path)
    )

    new_watermark = builtins.max(days_to_process)

    write_watermark(root, new_watermark)

    print(f"✅ Gold incremental write complete -> {gold_path}")
    print("Processed dates:", days_to_process)
    print("Updated watermark to:", new_watermark)
    print("Gold rows written this run:", daily.count())

    spark.stop()


if __name__ == "__main__":
    main()
