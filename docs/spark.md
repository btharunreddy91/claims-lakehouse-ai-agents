# Day 3 — Spark Structured Streaming: Bronze → Silver (Delta Lake)

## What it does

`src/streaming/bronze_to_silver.py` is the real Structured Streaming job.
It reads Bronze JSONL (partitioned by `filed_date`) as a stream and:

| Rule | How |
|---|---|
| Type casting | Schema enforces it: `claim_amount` → Double, `filed_date` / `incident_date` → Date, `ingested_at` → Timestamp |
| Dedupe | `dropDuplicates(["claim_id"])` — Kafka is at-least-once, so the same claim can arrive twice |
| Late data | `withWatermark("event_time", "7 days")` on `filed_date` as event time — rows arriving more than 7 days late are dropped by the engine |
| Sink | Delta Lake table at `data/silver`, partitioned by `filed_date`, `outputMode("append")`, checkpoint at `data/checkpoints/bronze_to_silver` |

The same rules live in `src/streaming/silver_transform.py`, a
Spark-free mirror that runs anywhere (see below). Day 4 builds on
Silver: cleaning + enrichment (policy/customer joins, derived fields).

## How to run (live — needs Java 17 + Spark)

```bash
pip install pyspark==3.5.3 delta-spark==3.2.0
# Java 17 must be on PATH (java -version). No Java = no Spark.

# One-shot (trigger once — processes everything currently in Bronze, then stops):
spark-submit --packages io.delta:delta-spark_2.12:3.2.0 \
    -m src.streaming.bronze_to_silver \
    --bronze data/bronze --silver data/silver --trigger once
# or: make silver-spark

# Continuous (micro-batches every minute — leave running):
spark-submit --packages io.delta:delta-spark_2.12:3.2.0 \
    -m src.streaming.bronze_to_silver \
    --bronze data/bronze --silver data/silver --trigger continuous

# Inspect the Delta table:
ls data/silver/                       # _delta_log/ + filed_date=... partitions
```

## Spark-free dry-run (this VM has no Java / Spark)

```bash
make silver
# python -m src.streaming.silver_transform --bronze data/bronze --silver data/silver
```

The mirror reads the same Bronze files, applies casting, validation,
dedupe (latest `ingested_at` wins) and the 7-day watermark, and writes
Silver JSONL partitioned by `filed_date`. Invalid rows go to
`data/silver/_quarantine/bad_rows.jsonl`; late rows to
`data/silver/_late/late_rows.jsonl` — neither is silently dropped.
It proves the transform logic; it does **not** prove the Spark job
runs. Run the live `spark-submit` above on a machine with Java before
calling Day 3 fully verified end-to-end.

## Failure modes

- **No Java / PySpark missing:** the job exits code 2 with
  "PySpark / delta-spark not installed" and points at `make silver`.
  It never pretends the stream ran.
- **Bad record in Bronze:** the streaming schema casts what it can;
  rows that fail `Claim.validate()` are quarantined by the mirror and
  by Day 4 cleaning — never silently dropped.
- **Duplicate delivery:** deduped by `claim_id` in the stream, so a
  re-delivered Kafka record lands once in Silver.
- **Late data:** a claim whose `filed_date` is more than 7 days older
  than the newest `filed_date` seen *so far* in the stream is dropped
  by the watermark (recorded in `_late/` by the mirror). Because the
  watermark advances with arrival order, a backfill arriving in date
  order is not wiped out — only a claim landing in a much newer
  partition than its own date is late. Raise `--watermark-days` if
  replays need a longer window — at the cost of more streaming state.
- **Restart:** the checkpoint at `data/checkpoints/bronze_to_silver`
  resumes where the query left off; deleting it reprocesses Bronze
  from the start (dedupe keeps Silver correct, but it is a full replay).
