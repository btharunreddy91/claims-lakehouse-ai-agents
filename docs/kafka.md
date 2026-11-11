# Day 2 — Kafka (Redpanda): claims.raw

## Why Redpanda, not plain Kafka?

Redpanda is Kafka-protocol compatible, runs as a single container with no
Zookeeper / KRaft setup, and speaks the same `kafka` client code
(`kafka-python-ng`, the maintained fork of `kafka-python` — the original
2.0.2 does not import on Python 3.12). Swap `localhost:19092` for a real
Kafka broker later and nothing in `src/generator/kafka_producer.py` or
`src/ingestion/kafka_consumer.py` changes. Day 3 (Spark Structured
Streaming) reads the same topic.

## Topic design

| Setting | Value | Why |
|---|---|---|
| Topic | `claims.raw` | Raw, as-received claims — the Bronze entry point |
| Partitions | 3 | Parallelism for the Day 3 streaming job; small enough to reason about locally |
| Key | `claim_id` | Same claim always lands on the same partition (ordering per claim); stable, non-PII key |
| Value | JSON, UTF-8 | Human-readable, matches the Day 1 JSONL format; schema is `src.common.schemas.Claim` |
| Retention | 7 days (default in compose) | Raw replay window for reprocessing; Bronze is the durable copy |
| Acks | `all` | Producer waits for all in-sync replicas before confirming |
| Consumer group | `claims-bronze-loader` | One group for the Bronze loader; add a group per downstream consumer |

Schema = the Claim fields in `src/common/schemas.py`:
`claim_id, policy_id, customer_id, claim_type, status, claim_amount,
filed_date, incident_date, state, description` (+ `customer_name`, PII —
masked in Gold, Day 5). No Schema Registry yet — that is a Day 11/12
productionising item, not a Day 2 blocker.

## How to run (live)

```bash
docker compose up -d          # Redpanda (19092) + Postgres (5432)
# wait for Redpanda: curl -s localhost:9644/v1/status/ready

# Create the topic explicitly (or let the producer auto-create it):
docker exec claims-redpanda rpk topic create claims.raw -p 3 -r 1
docker exec claims-redpanda rpk topic list
docker exec claims-redpanda rpk topic describe claims.raw

# Produce 100 claims, then consume them into Bronze:
python -m src.generator.kafka_producer --count 100
python -m src.ingestion.kafka_consumer --count 100 --to-bronze
# or: make produce && make consume

# Inspect what's in the topic:
docker exec claims-redpanda rpk topic consume claims.raw --num 2
```

## Broker-free dry-run (this VM has no Docker)

```bash
python -m src.generator.kafka_producer --count 100 --dry-run
python -m src.ingestion.kafka_consumer --count 100 --dry-run --to-bronze
```

Dry-run writes the exact same records the producer would send
(`{"key": claim_id, "value": {...}}` per line) to
`data/raw/claims_kafka.jsonl`, and the consumer reads that file back
through the same validation / Bronze code path. It proves the
serialisation, validation, and Bronze landing — it does **not** prove
the broker works. Run the live commands above on a machine with Docker
before calling Day 2 fully verified end-to-end.

## Failure modes

- **Broker down:** producer/consumer exit code 2 with
  "could not connect to Kafka at localhost:19092 — Is Redpanda running?"
  They never hang silently or report success.
- **Bad record:** validated by `Claim.validate()`; quarantined to
  `data/bronze/_quarantine/bad_rows.jsonl` (with `--to-bronze`) or
  `kafka_bad_rows.jsonl` (without), never silently dropped.
- **Duplicate delivery:** Kafka is at-least-once. Dedupe by `claim_id`
  happens in Day 3 (Spark Streaming / Delta merge), not here.
