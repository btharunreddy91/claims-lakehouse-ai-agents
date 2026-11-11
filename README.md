# Claims Lakehouse + AI Agents

**Real-time Insurance Claims Lakehouse with a Multi-Agent AI layer.**
Built a little every day — not a weekend dump.

> Domain: Insurance (claims, policy, billing).
> Stack: Python · Kafka (Redpanda) · PySpark Structured Streaming · Delta Lake / Medallion (Bronze → Silver → Gold) · dbt · Snowflake · Postgres · Great Expectations · LangGraph / LangChain Agents · FastAPI

## The problem it solves

Claims arrive as a messy, high-volume stream. Business users ask questions in English
("Which claim types spiked in Texas this week?") and engineers spend hours writing
the same Spark / SQL / dbt code, fixing quality issues, and explaining numbers.

This repo builds **one system that does both**:

1. **A real lakehouse pipeline** — streaming claims in, cleaned and modelled, ready for BI.
2. **Agents on top of it** — agents that *build* pipelines, *check* data quality,
   and *answer* questions over the Gold layer, with guardrails and lineage.

## Architecture

```
                ┌──────────────────┐
  Policies /    │  Claims Generator│  synthetic, realistic
  Customers ──► │  (Kafka producer)│ ──► Redpanda / Kafka ──┐
  Claims        └──────────────────┘                        │
                                                            ▼
                                              ┌─────────────────────┐
                                              │ Spark Structured    │
                                              │ Streaming           │
                                              │ Bronze → Silver     │
                                              └─────────┬───────────┘
                                                        │
                                              ┌─────────▼───────────┐
                                              │ Delta Lake          │
                                              │ bronze / silver /   │
                                              │ gold (dbt marts)    │
                                              └─────────┬───────────┘
                                                        │
                        ┌───────────────────────────────┼───────────────────────────────┐
                        ▼                               ▼                               ▼
              ┌─────────────────┐             ┌─────────────────┐             ┌─────────────────┐
              │ Pipeline Agent  │             │ Quality Agent   │             │ Analytics Agent │
              │ NL → PySpark /  │             │ Great Expectations│             │ NL → SQL over   │
              │ dbt, with tests │             │ + auto-quarantine│             │ Gold, with      │
              └─────────────────┘             └─────────────────┘             │ guardrails      │
                        └───────────────────────────────┴───────────────────────────────┘
                                              Supervisor (LangGraph)
                                              FastAPI + UI
```

## What each agent does — "how to create agents"

Agents here are **not chatbots**. Each is a LangGraph state machine with:
`tools + state + guardrails + human-in-the-loop for risky steps`.

| Agent | Tools | Guardrail |
|---|---|---|
| **Pipeline Agent** | read schema, generate Spark/dbt, run tests | Never writes to prod tables; only proposes a PR / dry-run |
| **Quality Agent** | Great Expectations suites, profile data, quarantine | Auto-quarantine on fail, never silently drops rows |
| **Analytics Agent** | Text-to-SQL over Gold only, run read-only query | Read-only, Gold only, row-limit + PII masking, cites SQL |

See `docs/agents.md` (Day 6+) for the full pattern.

## Quickstart — Day 1 (no Docker / Spark needed)

```bash
git clone <your-github-remote>   # add remote later — see below
cd claims-lakehouse-ai-agents
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Generate 1,000 synthetic claims into Bronze (partitioned by date)
python -m src.generator.generate_claims --count 1000

# Ingest a JSONL file into Bronze with schema validation
python -m src.ingestion.bronze_ingest --input data/raw/claims.jsonl

# Run tests
pytest -q
```

## Quickstart — Day 2 (Kafka / Redpanda)

```bash
docker compose up -d   # Redpanda (Kafka) + Postgres
python -m src.generator.kafka_producer --count 100
python -m src.ingestion.kafka_consumer --count 100 --to-bronze
# or: make produce && make consume
```

No Docker? Use the broker-free dry-run (same records, same validation
and Bronze code path — it does not prove the broker works):

```bash
python -m src.generator.kafka_producer --count 100 --dry-run
python -m src.ingestion.kafka_consumer --count 100 --dry-run --to-bronze
```

See [docs/kafka.md](docs/kafka.md) for topic design (`claims.raw`, 3
partitions, key `claim_id`), `rpk` commands, and failure modes.

## Repo layout

```
src/
  generator/     synthetic claims / policies / customers (Kafka producer)
  ingestion/     Bronze ingestion + schema validation
  streaming/     Spark Structured Streaming: Bronze → Silver
  dbt/           dbt models: staging → marts (Gold)
  quality/       Great Expectations suites + quarantine
  agents/        LangGraph agents: pipeline, quality, analytics, supervisor
  common/        schemas, config, logging
tests/           pytest — generator, ingestion, quality
data/            local lakehouse (gitignored) — bronze / silver / gold
docs/            architecture, agents, decisions (ADRs)
ROADMAP.md       the daily build plan — one slice per day
```

## Daily build plan

See [ROADMAP.md](ROADMAP.md). ~30–45 min/day, each day ends with something
that runs and a commit. No big-bang days.

| Days | Slice |
|---|---|
| 1–2 | Repo, generator, Bronze, Kafka |
| 3–4 | Spark Streaming Bronze → Silver, Delta |
| 5 | dbt Gold marts |
| 6–8 | Agents: Analytics (Text-to-SQL), Quality, Pipeline |
| 9–10 | Supervisor + FastAPI, guardrails, evals |
| 11–12 | Snowflake load, observability, CI |
| 13–14 | Demo, README polish, interview talking points |

## Pushing to GitHub (when you're ready)

```bash
# Create an empty repo on github.com, then:
git remote add origin git@github.com:<you>/claims-lakehouse-ai-agents.git
git push -u origin main
```

No secrets in this repo. Ever. See `.gitignore` + `docs/security.md` later.
