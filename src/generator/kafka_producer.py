"""Day 2 — Kafka producer for claims.raw.

Reuses the Day 1 synthetic generator (_claim) and publishes each claim
as JSON to topic ``claims.raw``, key = claim_id.

Usage (live — needs Redpanda running):
    docker compose up -d
    python -m src.generator.kafka_producer --count 100

Dry-run (no broker needed — writes the same records to a JSONL file
that kafka_consumer --dry-run can read back):
    python -m src.generator.kafka_producer --count 100 --dry-run
    python -m src.generator.kafka_producer --count 100 --dry-run --out data/raw/claims_kafka.jsonl

If the broker is down, this exits with a clear error (exit code 2)
instead of hanging or silently succeeding.
"""
from __future__ import annotations

import argparse
import json
import random
import sys
from pathlib import Path

from src.common.kafka_utils import DEFAULT_BOOTSTRAP, TOPIC, to_kafka_record
from src.generator.generate_claims import _claim


def produce_dry_run(count: int, seed: int, out: Path) -> int:
    """Write ``count`` claims as Kafka records to ``out`` (one JSON per line:
    {"key": claim_id, "value": {...}}). Returns the number written."""
    rng = random.Random(seed)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w") as f:
        for i in range(1, count + 1):
            claim = _claim(rng, i)
            key, value = to_kafka_record(claim)
            f.write(json.dumps({"key": key.decode(), "value": json.loads(value)}) + "\n")
    return count


def produce(count: int, seed: int, bootstrap: str, topic: str) -> int:
    """Publish ``count`` claims to Kafka. Returns the number sent."""
    try:
        from kafka import KafkaProducer
        from kafka.errors import KafkaTimeoutError, NoBrokersAvailable
    except ImportError as e:
        print(f"ERROR: kafka-python-ng is not installed: {e}\n"
              "Run: pip install -r requirements.txt", file=sys.stderr)
        raise SystemExit(2) from e

    rng = random.Random(seed)
    try:
        producer = KafkaProducer(
            bootstrap_servers=[bootstrap],
            key_serializer=lambda k: k,
            value_serializer=lambda v: v,
            acks="all",
            retries=3,
            # Fail fast when no broker is reachable instead of blocking
            # for the 60 s default: producer is lazy, so the broker
            # check happens on first send / flush.
            api_version_auto_timeout_ms=5000,
            request_timeout_ms=10000,
            max_block_ms=10000,
        )
        sent = 0
        for i in range(1, count + 1):
            key, value = to_kafka_record(_claim(rng, i))
            producer.send(topic, key=key, value=value)
            sent += 1
        producer.flush(timeout=15)
    except (NoBrokersAvailable, KafkaTimeoutError) as e:
        print(f"ERROR: could not connect to Kafka at {bootstrap}: {e}\n"
              "Is Redpanda running? Run: docker compose up -d\n"
              "Or use --dry-run for a broker-free run.", file=sys.stderr)
        raise SystemExit(2) from e
    finally:
        try:
            producer.close()
        except NameError:
            pass
    return sent


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--count", type=int, default=100)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    ap.add_argument("--topic", default=TOPIC)
    ap.add_argument("--dry-run", action="store_true",
                    help="write records to a JSONL file instead of Kafka")
    ap.add_argument("--out", default="data/raw/claims_kafka.jsonl")
    args = ap.parse_args()

    if args.dry_run:
        n = produce_dry_run(args.count, args.seed, Path(args.out))
        print(f"Dry-run: wrote {n} records → {args.out} (topic {args.topic})")
    else:
        n = produce(args.count, args.seed, args.bootstrap, args.topic)
        print(f"Produced {n} claims → topic {args.topic} @ {args.bootstrap}")


if __name__ == "__main__":
    main()
