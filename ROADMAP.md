# Daily Build Plan — ~30–45 min/day

Rule: **each day ships one runnable slice + one commit.** No day tries to do everything.

## Week 1 — The Lakehouse

- [x] **Day 1 — Repo + Generator + Bronze**
  Repo init, architecture, synthetic claims generator (policies, customers,
  claims with realistic distributions), Bronze ingestion with schema
  validation, partitioned local lakehouse, tests.
- [ ] **Day 2 — Kafka (Redpanda)**
  `docker-compose.yml` (Redpanda + Postgres), producer publishes to
  `claims.raw`, consumer reads it back, topic / schema notes.
- [ ] **Day 3 — Spark Streaming: Bronze → Silver**
  Structured Streaming job, Delta Lake, dedupe by `claim_id`, type casting,
  late-data watermark.
- [ ] **Day 4 — Silver cleaning + enrichment**
  Join policies/customers, standardise state / claim-type, derive
  `claim_age_days`, `is_high_value`, quarantine bad rows.
- [ ] **Day 5 — dbt Gold marts**
  `stg_claims`, `fct_claims`, `dim_policy`, `dim_customer`,
  `agg_claims_daily_state`. dbt tests (unique, not_null, accepted values).

## Week 2 — The Agents

- [ ] **Day 6 — Agent pattern + Analytics Agent v1**
  LangGraph 101 in this repo: state, nodes, tools. Text-to-SQL over Gold
  only, read-only, with SQL shown to the user.
- [ ] **Day 7 — Quality Agent**
  Great Expectations suite, profiling, auto-quarantine, Slack-style report.
- [ ] **Day 8 — Pipeline Agent**
  Natural language → PySpark / dbt draft, generated tests, dry-run only,
  human approves before anything lands.
- [ ] **Day 9 — Supervisor + FastAPI**
  LangGraph supervisor routes to the right agent, FastAPI `/ask`, `/pipeline`,
  `/quality` endpoints, simple UI.
- [ ] **Day 10 — Guardrails + Evals**
  PII masking, row limits, prompt-injection checks, a small eval set
  (10 questions, expected SQL / answers) run in CI.

## Week 3 — Productionise + Tell the story

- [ ] **Day 11 — Snowflake + Postgres sink**
  Gold → Snowflake (or Postgres for local demo), incremental load.
- [ ] **Day 12 — Observability + CI**
  Structured logging, run metrics, GitHub Actions (pytest + dbt test + lint).
- [ ] **Day 13 — Demo dataset + screenshots**
  A 2-minute demo script: stream → Silver → ask the agent a question.
- [ ] **Day 14 — Polish**
  README, architecture diagram, interview talking points (`docs/interview.md`):
  what you built, trade-offs, what you'd do at scale.

## How we work each day

1. You say "day N" (or just "continue").
2. I build that day's slice, run it, and commit it here.
3. You review the diff / run it yourself if you want — 5 minutes.
4. Next day, next slice. If a day is skipped, we just pick up where we left off.
