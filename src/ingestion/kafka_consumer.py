"""Day 2 — Kafka consumer for claims.raw.

Consumes claims.raw, validates each record with Claim.validate(), and
counts valid vs quarantined. Bad rows are NEVER silently dropped:
- with --to-bronze, all rows go through bronze_ingest.ingest(), which
  partitions valid rows by filed_date and writes bad rows to
  data/bronze/_quarantine/bad_rows.jsonl;
- without --to-bronze, bad rows are written to
  data/bronze/_quarantine/kafka_bad_rows.jsonl and reported.

Usage (live — needs Redpanda running, producer already run):
    python -m src.ingestion.kafka_consumer --count 100 --to-bronze
    python -m src.ingestion.kafka_consumer --count 100 --timeout-secs 30

Dry-run (no broker — reads the producer's --dry-run JSONL file):
    python -m src.generator.kafka_producer --count 100 --dry-run
    python -m src.ingestion.kafka_consumer --count 100 --dry-run --to-bronze
"""
from __future__ import annotations

import argparse
import json
import sys
import tempfile
from collections import Counter
from pathlib import Path

from src.common.kafka_utils import (
    DEFAULT_BOOTSTRAP,
    DEFAULT_GROUP,
    TOPIC,
    deserialize_claim,
    validate_record,
)
from src.ingestion.bronze_ingest import ingest


def _handle_rows(rows: list[dict], to_bronze: bool, bronze_dir: Path) -> dict:
    """Validate rows, optionally land them in Bronze. Returns counts."""
    counts: Counter = Counter()
    valid: list[dict] = []
    bad: list[dict] = []
    for row in rows:
        counts["read"] += 1
        errors = validate_record(row)
        if errors:
            counts["quarantined"] += 1
            bad.append({"row": row, "errors": errors})
        else:
            counts["valid"] += 1
            valid.append(row)

    if to_bronze:
        # Reuse Day 1 ingest() on ALL rows so bad rows land in quarantine
        # via the same code path as the file-based Bronze load.
        with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as tmp:
            for row in rows:
                tmp.write(json.dumps(row) + "\n")
            tmp_path = Path(tmp.name)
        try:
            bronze_counts = ingest(tmp_path, bronze_dir)
            # ingest() recounts the same rows; prefer its counts.
            counts = Counter(bronze_counts)
        finally:
            tmp_path.unlink(missing_ok=True)
    elif bad:
        q = bronze_dir / "_quarantine"
        q.mkdir(parents=True, exist_ok=True)
        with (q / "kafka_bad_rows.jsonl").open("w") as f:
            for b in bad:
                f.write(json.dumps(b) + "\n")
        print(f"Quarantined {len(bad)} bad row(s) → {q / 'kafka_bad_rows.jsonl'}")

    if valid and not to_bronze:
        print(f"Valid: {len(valid)} (not landed — re-run with --to-bronze to land them)")
    return dict(counts)


def consume_dry_run(input_path: Path, count: int, to_bronze: bool,
                    bronze_dir: Path) -> dict:
    """Read Kafka records from a producer --dry-run JSONL file."""
    rows: list[dict] = []
    for line in input_path.read_text().splitlines():
        if len(rows) >= count:
            break
        if not line.strip():
            continue
        try:
            rec = json.loads(line)
            rows.append(rec["value"])
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            rows.append({"_raw": line, "_error": str(e)})
    return _handle_rows(rows, to_bronze, bronze_dir)


def consume(count: int, timeout_secs: int, bootstrap: str, topic: str,
            group: str, to_bronze: bool, bronze_dir: Path) -> dict:
    """Consume up to ``count`` records from Kafka (or until timeout)."""
    try:
        from kafka import KafkaConsumer
        from kafka.errors import NoBrokersAvailable
    except ImportError as e:
        print(f"ERROR: kafka-python-ng is not installed: {e}\n"
              "Run: pip install -r requirements.txt", file=sys.stderr)
        raise SystemExit(2) from e

    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[bootstrap],
            group_id=group,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=timeout_secs * 1000,
            value_deserializer=lambda b: b,  # decode via deserialize_claim
        )
    except NoBrokersAvailable as e:
        print(f"ERROR: could not connect to Kafka at {bootstrap}: {e}\n"
              "Is Redpanda running? Run: docker compose up -d\n"
              "Or use --dry-run for a broker-free run.", file=sys.stderr)
        raise SystemExit(2) from e

    rows: list[dict] = []
    try:
        for msg in consumer:  # first poll is where a dead broker surfaces
            try:
                rows.append(deserialize_claim(msg.value))
            except ValueError as e:
                rows.append({"_raw": msg.value.decode("utf-8", "replace"),
                             "_error": str(e)})
            if len(rows) >= count:
                break
        consumer.commit()
    except NoBrokersAvailable as e:
        print(f"ERROR: could not connect to Kafka at {bootstrap}: {e}\n"
              "Is Redpanda running? Run: docker compose up -d\n"
              "Or use --dry-run for a broker-free run.", file=sys.stderr)
        raise SystemExit(2) from e
    finally:
        consumer.close()
    return _handle_rows(rows, to_bronze, bronze_dir)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--count", type=int, default=100)
    ap.add_argument("--timeout-secs", type=int, default=30)
    ap.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    ap.add_argument("--topic", default=TOPIC)
    ap.add_argument("--group", default=DEFAULT_GROUP)
    ap.add_argument("--to-bronze", action="store_true",
                    help="land valid rows in Bronze (bad rows quarantined)")
    ap.add_argument("--bronze", default="data/bronze")
    ap.add_argument("--dry-run", action="store_true",
                    help="read a producer --dry-run JSONL file instead of Kafka")
    ap.add_argument("--input", default="data/raw/claims_kafka.jsonl")
    args = ap.parse_args()

    if args.dry_run:
        result = consume_dry_run(Path(args.input), args.count, args.to_bronze,
                                 Path(args.bronze))
        print(f"Dry-run consume from {args.input}: {result}")
    else:
        result = consume(args.count, args.timeout_secs, args.bootstrap,
                         args.topic, args.group, args.to_bronze, Path(args.bronze))
        print(f"Consumed from topic {args.topic} @ {args.bootstrap}: {result}")


if __name__ == "__main__":
    main()
