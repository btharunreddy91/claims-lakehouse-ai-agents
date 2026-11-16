# Daily Build Plan — ~30–45 min/day

Rule: **each day ships one runnable slice + one commit.** No day tries to do everything.

## Week 1 — The Lakehouse

- [x] **Day 1 — Repo + Generator + Bronze**
  Repo init, architecture, synthetic claims generator (policies, customers,
  claims with realistic distributions), Bronze ingestion with schema
  validation, partitioned local lakehouse, tests.
- [x] **Day 2 — Kafka (Redpanda)**
  `docker-compose.yml` (Redpanda + Postgres), producer publishes to
  `claims.raw`, consumer reads it back, topic / schema notes.
  Done Nov 11: `kafka_producer.py` + `kafka_consumer.py` (key=claim_id,
  validation + quarantine, `--to-bronze`), `docs/kafka.md`, broker-free
  tests. Live Redpanda run NOT yet verified — no Docker in this VM;
  dry-run round-trip verified instead (see docs/kafka.md).
- [x] **Day 3 — Spark Streaming: Bronze → Silver**
  Structured Streaming job, Delta Lake, dedupe by `claim_id`, type casting,
  late-data watermark.
  Done Nov 12: `src/streaming/bronze_to_silver.py` (real job, Delta,
  7-day watermark, checkpoint) + `src/streaming/silver_transform.py`
  (Spark-free mirror of the same rules) + `docs/spark.md`, 8/8 new
  tests passing. Live Spark run NOT yet verified — no Java in this VM;
  dry-run over the real Bronze files verified instead (see docs/spark.md).
- [x] **Day 4 — Silver cleaning + enrichment**
  Join policies/customers, standardise state / claim-type, derive
  `claim_age_days`, `is_high_value`, quarantine bad rows.
  Done Nov 13: `src/streaming/silver_enrich.py` (Spark-free mirror) +
  `docs/silver_enrichment.md` (Spark SQL equivalent, not run here —
  no Java) + 11 new tests. Dry-run over real Silver: 758 read /
  758 enriched / 0 quarantined, 166 high-value (>= $25k),
  725 dim customers / 755 dim policies derived from Silver itself
  (no external policy/customer master exists in this demo — counts
  are computed from the claims present, nothing invented).
- [x] **Day 5 — dbt Gold marts**
  `stg_claims`, `fct_claims`, `dim_policy`, `dim_customer`,
  `agg_claims_daily_state`. dbt tests (unique, not_null, accepted values).
  Done Nov 14: real dbt project in `src/dbt` (dbt-duckdb profile) +
  stdlib mirror `src/dbt/build_gold.py` + `docs/dbt_gold.md`, 6 new tests.
  Verified over real enriched Silver: 758 fct / 725 customers /
  755 policies / 716 agg rows, totals reconcile at $11,975,925.09.
  `dbt build` itself NOT run here — no dbt/DuckDB in this VM.

## Week 2 — The Agents

- [x] **Day 6 — Agent pattern + Analytics Agent v1**
  LangGraph 101 in this repo: state, nodes, tools. Text-to-SQL over Gold
  only, read-only, with SQL shown to the user.
  Done Nov 15: `src/agents/analytics.py` (typed state, plan → validate →
  execute → answer, rule-based offline planner + `plan_with_llm` seam,
  read-only validator, row limit 100/1000, PII masking) + `src/agents/cli.py`
  + `docs/agents.md`, 50 new tests. Real LangGraph 1.2.22 in the repo
  `.venv` (verified); stdlib mirror fallback when it is not importable
  (system python3 cannot install it — PEP 668 / Debian typing_extensions),
  mirror returns identical rows/answers. Verified over real Gold: 758
  claims, $11,975,925.09 total, TX top state $2,743,287.02, avg $15,799.37.
  No LLM called in v1; unknown questions return "not supported yet"
  instead of guessing SQL.
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
