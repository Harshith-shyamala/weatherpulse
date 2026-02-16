from __future__ import annotations

import json
from pathlib import Path
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, year, month, dayofmonth, hour, current_timestamp
)


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


STATE_FILE = "data/_state/silver_watermark.json"


def read_watermark(root: Path) -> str | None:
    p = root / STATE_FILE
    if not p.exists():
        return None
    obj = json.loads(p.read_text(encoding="utf-8"))
    return obj.get("last_dt")


def write_watermark(root: Path, last_dt: str) -> None:
    p = root / STATE_FILE
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"last_dt": last_dt}, indent=2), encoding="utf-8")


def list_bronze_dts(root: Path) -> list[str]:
    bronze_base = root / "data/bronze/openmeteo_hourly"
    if not bronze_base.exists():
        return []
    dts = []
    for p in bronze_base.glob("dt=*"):
        if p.is_dir():
            dts.append(p.name.split("dt=")[-1])
    return sorted(dts)


def main():
    root = project_root()
    bronze_base = root / "data/bronze/openmeteo_hourly"
    silver_path = str(root / "data/silver/openmeteo_hourly")

    spark = SparkSession.builder.appName("weatherpulse-bronze-to-silver-incremental").getOrCreate()
    # ✅ dynamic overwrite = overwrite only partitions present in this write
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    last_dt = read_watermark(root)
    all_dts = list_bronze_dts(root)

    if not all_dts:
        raise RuntimeError(f"No Bronze dt partitions found under: {bronze_base}")

    # Pick only NEW dt partitions
    if last_dt is None:
        dts_to_process = all_dts  # first run: process everything
    else:
        dts_to_process = [d for d in all_dts if d > last_dt]

    if not dts_to_process:
        print(f"✅ No new Bronze partitions. Watermark={last_dt}. Nothing to do.")
        spark.stop()
        return

    # Read only those dt partitions
    paths = [str(bronze_base / f"dt={d}") for d in dts_to_process]
    df = spark.read.option("basePath", str(bronze_base)).parquet(*paths)


    df2 = (
        df
        .withColumn("event_time", to_timestamp(col("event_time")))
        .withColumn("temperature_2m", col("temperature_2m").cast("double"))
        .withColumn("relative_humidity_2m", col("relative_humidity_2m").cast("double"))
        .withColumn("precipitation", col("precipitation").cast("double"))
        .withColumn("cloud_cover", col("cloud_cover").cast("double"))
        .withColumn("wind_speed_10m", col("wind_speed_10m").cast("double"))
        .withColumn("silver_processed_at", current_timestamp())
        .withColumn("year", year(col("event_time")))
        .withColumn("month", month(col("event_time")))
        .withColumn("day", dayofmonth(col("event_time")))
        .withColumn("hour", hour(col("event_time")))
        .dropDuplicates(["location", "event_time"])
        .withColumn("location_part", col("location"))
    )

    # ✅ overwrite only the partitions touched by this batch (dynamic mode)
    (
        df2.write
        .mode("overwrite")
        .partitionBy("year", "month", "day", "location_part")
        .parquet(silver_path)
    )

    new_watermark = max(dts_to_process)
    write_watermark(root, new_watermark)

    print(f"✅ Silver incremental write complete -> {silver_path}")
    print("Processed dt partitions:", dts_to_process)
    print("Updated watermark to:", new_watermark)

    spark.stop()


if __name__ == "__main__":
    main()
