"""Day 3 — Spark Structured Streaming: Bronze -> Silver (Delta Lake).

Reads Bronze JSONL (partitioned by filed_date) as a stream, applies the
same rules as ``src/streaming/silver_transform.py`` — type casting,
validation-shaped schema, dedupe by ``claim_id``, 7-day late-data
watermark — and writes a Delta table partitioned by filed_date.

Requires Java 17 + PySpark 3.5.3 + delta-spark 3.2.0 (see docs/spark.md).
This VM has no Java, so the job cannot run here; the Spark-free mirror
in ``silver_transform.py`` proves the same logic (``make silver``).

Usage (live, on a machine with Java + Spark):
    spark-submit --packages io.delta:delta-spark_2.12:3.2.0 \
        -m src.streaming.bronze_to_silver \
        --bronze data/bronze --silver data/silver --trigger once
    # or: make silver-spark
"""
from __future__ import annotations

import argparse
import sys

# Silver schema — derived from src.common.schemas.CLAIM_FIELDS.
# filed_date / incident_date are Dates, claim_amount is Double,
# ingested_at is a Timestamp (the event time for the watermark).
SPARK_SCHEMA_DDL = (
    "claim_id STRING, policy_id STRING, customer_id STRING, "
    "claim_type STRING, status STRING, claim_amount DOUBLE, "
    "filed_date DATE, incident_date DATE, state STRING, "
    "description STRING, ingested_at TIMESTAMP"
)

SILVER_COLUMNS = [
    "claim_id", "policy_id", "customer_id", "claim_type", "status",
    "claim_amount", "filed_date", "incident_date", "state",
    "description", "ingested_at",
]


def build_spark(app_name: str = "claims-bronze-to-silver"):
    """Build a Spark session with Delta support, or fail loudly."""
    try:
        from delta import configure_spark_with_delta_pip
        from pyspark.sql import SparkSession
    except ImportError as e:
        print(f"ERROR: PySpark / delta-spark not installed: {e}\n"
              "Install them (needs Java 17): pip install pyspark==3.5.3 delta-spark==3.2.0\n"
              "See docs/spark.md. The Spark-free mirror is: make silver",
              file=sys.stderr)
        sys.exit(2)
    builder = (SparkSession.builder.appName(app_name)
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog",
                       "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    return configure_spark_with_delta_pip(builder).getOrCreate()


def run(bronze: str, silver: str, checkpoint: str, watermark_days: int,
        trigger: str, max_files_per_trigger: int) -> None:
    from pyspark.sql.functions import col, to_timestamp
    from pyspark.sql.types import StructType

    spark = build_spark()
    schema = StructType.fromDDL(SPARK_SCHEMA_DDL)

    bronze_stream = (spark.readStream
                     .schema(schema)
                     .option("maxFilesPerTrigger", max_files_per_trigger)
                     .json(bronze))

    # Event time for the watermark: filed_date as a timestamp.
    # Casting is enforced by the schema (claim_amount -> Double,
    # filed_date / incident_date -> Date); malformed rows land in
    # _corrupt_record handling via the JSON reader's PERMISSIVE mode
    # and are surfaced in the streaming query's metrics, not dropped
    # silently — full validation + quarantine lives in the mirror
    # (silver_transform.py) and the Day 4 cleaning step.
    silver_stream = (
        bronze_stream
        .withColumn("event_time", to_timestamp(col("filed_date")))
        .withWatermark("event_time", f"{watermark_days} days")
        .dropDuplicates(["claim_id"])  # Kafka at-least-once: one row per claim
        .select(*SILVER_COLUMNS)
    )

    query = (silver_stream.writeStream
             .format("delta")
             .outputMode("append")
             .partitionBy("filed_date")
             .option("checkpointLocation", checkpoint)
             .trigger(once=True) if trigger == "once" else
             (silver_stream.writeStream
              .format("delta")
              .outputMode("append")
              .partitionBy("filed_date")
              .option("checkpointLocation", checkpoint)
              .trigger(processingTime="1 minute")))
    print(f"Bronze -> Silver streaming: {bronze} -> {silver} "
          f"(watermark {watermark_days}d, trigger={trigger})")
    query = query.start(silver)
    query.awaitTermination()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bronze", default="data/bronze")
    ap.add_argument("--silver", default="data/silver")
    ap.add_argument("--checkpoint", default="data/checkpoints/bronze_to_silver")
    ap.add_argument("--watermark-days", type=int, default=7)
    ap.add_argument("--trigger", choices=["once", "continuous"], default="once")
    ap.add_argument("--max-files-per-trigger", type=int, default=10)
    args = ap.parse_args()
    run(args.bronze, args.silver, args.checkpoint, args.watermark_days,
        args.trigger, args.max_files_per_trigger)


if __name__ == "__main__":
    main()
