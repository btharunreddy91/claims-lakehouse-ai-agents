# Day 5 — dbt Gold marts

## What it does

| Model | Grain | Source |
|---|---|---|
| `stg_claims` | 1 row / claim | `silver.silver_enriched` (Day 4) |
| `fct_claims` | 1 row / claim | `stg_claims` |
| `dim_customer` | 1 row / customer | claims themselves (no external master) |
| `dim_policy` | 1 row / policy | claims themselves (no external master) |
| `agg_claims_daily_state` | 1 row / (filed_date, state, claim_type) | `stg_claims` |

dbt tests in `models/marts/schema.yml` + `models/staging/sources.yml`:
`unique` + `not_null` on `claim_id` (fact, dims), `accepted_values` on
`claim_type` (`auto`, `property`, `health`, `life`) and `status`
(`filed`, `in_review`, `approved`, `denied`, `paid`, `closed`).

## How to run — real dbt (needs dbt, not in this VM)

```bash
pip install dbt-duckdb
cd src/dbt
# Point DuckDB at the enriched Silver JSONL files first:
duckdb data/gold/claims.duckdb -c "
  CREATE OR REPLACE VIEW silver.silver_enriched AS
  SELECT * FROM read_json_auto('data/silver_enriched/filed_date=*/part.jsonl',
                               union_by_name => true);"
dbt build
```

## How to run — verified here (stdlib mirror)

```bash
make gold
# .venv/bin/python -m src.dbt.build_gold --silver data/silver_enriched --out data/gold
```

The mirror runs the same transformations the SQL declares and writes
`data/gold/{fct_claims,dim_customer,dim_policy,agg_claims_daily_state}.jsonl`
plus `manifest.json`. dbt itself is **not** installed in this VM, so
`dbt build` is documented, not run here — same pattern as Days 2–4
(Kafka / Spark documented, mirror verified).

## Verified result (Nov 14, over the real enriched Silver)

- 758 source rows → 758 `fct_claims` (no dedupe loss, no quarantine)
- 725 `dim_customer`, 755 `dim_policy` — matches the Day 4 counts
- 716 `agg_claims_daily_state` rows
- Reconciliation: agg claim count 758 = fct count 758;
  agg total $11,975,925.09 = fct total $11,975,925.09
- 166 high-value claims (>= $25k)
- Tests: 37 passed (31 existing + 6 new), including the reconciliation
  test over the real Silver data.

## Honesty notes

- `dim_customer` / `dim_policy` are derived from the claims present:
  first-seen name, first/last filed dates, counts and totals computed
  from the data at hand. Nothing about a policy or customer is invented.
- `dim_customer.customer_name` in SQL uses `arg_max(customer_name,
  filed_date)` (first-seen name); the mirror takes the first non-empty
  name in filed-date order — same rule.
- The Gold JSONL mirror and a future `dbt build` write to the same
  `data/gold/` directory; the DuckDB file (`claims.duckdb`) is created
  only by a real dbt run.
