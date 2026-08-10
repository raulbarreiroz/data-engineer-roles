"""Server-log lakehouse job (local-friendly sketch).

Bronze: raw JSONL logs
Silver: windowed session features + user-dim join
Output: parquet partitioned by event_date (Delta-ready)

Run from repo root:
    python jobs/process_server_logs.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]

LOG_INPUT = os.getenv("LOG_INPUT", str(ROOT / "sample_data" / "server_logs.jsonl"))
DIM_INPUT = os.getenv("DIM_INPUT", str(ROOT / "sample_data" / "dim_users.csv"))
OUTPUT_ROOT = os.getenv("OUTPUT_ROOT", str(ROOT / "output"))


def build_spark(app_name: str = "server-logs-lakehouse") -> SparkSession:
    # local[*] keeps this runnable on a laptop; swap master for yarn/k8s in prod
    return (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_bronze(spark: SparkSession):
    logs = (
        spark.read.json(LOG_INPUT)
        .withColumn("event_ts", F.to_timestamp("ts"))
        .withColumn("event_date", F.to_date("event_ts"))
        .drop("ts")
    )
    users = spark.read.option("header", True).csv(DIM_INPUT)
    return logs, users


def silver_sessions(logs, users):
    """30-min window aggregates per user, then join dim for plan/region."""

    w = (
        Window.partitionBy("user_id")
        .orderBy(F.col("event_ts").cast("long"))
        .rangeBetween(-30 * 60, 0)  # seconds looking back
    )

    featured = (
        logs.withColumn("events_30m", F.count("*").over(w))
        .withColumn("avg_latency_30m", F.avg("latency_ms").over(w))
        .withColumn("err_30m", F.sum(F.when(F.col("status") >= 500, 1).otherwise(0)).over(w))
        .withColumn(
            "session_rn",
            F.row_number().over(Window.partitionBy("user_id", "event_date").orderBy("event_ts")),
        )
    )

    # dim is tiny - broadcast keeps the join plan honest for demos
    joined = featured.join(F.broadcast(users), on="user_id", how="left")

    return joined.select(
        "user_id",
        "plan",
        "region",
        "host",
        "event",
        "event_ts",
        "event_date",
        "latency_ms",
        "status",
        "events_30m",
        "avg_latency_30m",
        "err_30m",
        "session_rn",
    )


def write_silver(df, out_dir: str) -> None:
    target = str(Path(out_dir) / "silver" / "server_sessions")
    Path(target).mkdir(parents=True, exist_ok=True)

    # Delta swap-in for Databricks/EMR:
    #   df.write.format("delta").mode("overwrite").partitionBy("event_date").save(target)
    (
        df.write.mode("overwrite")
        .partitionBy("event_date")
        .parquet(target)
    )
    print(f"wrote silver parquet -> {target}")


def main() -> int:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        logs, users = read_bronze(spark)
        silver = silver_sessions(logs, users)

        n = silver.count()
        print(f"silver rows: {n}")
        silver.show(10, truncate=False)

        write_silver(silver, OUTPUT_ROOT)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
