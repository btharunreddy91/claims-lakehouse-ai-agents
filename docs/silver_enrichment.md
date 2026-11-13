# Day 4 — Silver cleaning + enrichment

## What it does

`src/streaming/silver_enrich.py` is the Spark-free mirror; it reads Day 3
Silver (`data/silver`) and writes enriched Silver (`data/silver_enriched`),
partitioned by `filed_date`.

| Rule | How |
|---|---|
| Standardise `state` | strip/upper, full name → code ("Texas" → "TX"); unknown → quarantine |
| Standardise `claim_type` | lower/strip, synonyms ("automobile"/"car" → "auto", "home" → "property", "medical" → "health"); unknown → quarantine |
| Derive `claim_age_days` | `(filed_date - incident_date).days` |
| Derive `is_high_value` | `claim_amount >= 25,000.00` |
| Policy / customer join | No external policy/customer master exists in this demo — the source systems supply only IDs. `dim_customer` / `dim_policy` are therefore derived from Silver itself (first-seen name, first/last filed dates, claim count), and each row carries `customer_claim_count`, `policy_claim_count`, `is_repeat_customer`, `is_repeat_policy`. Nothing about a policy or customer is invented. Day 5 (dbt) formalises these as `dim_customer` / `dim_policy`. |
| Quarantine | Missing claim/policy/customer id, unparseable amount/date, incident after filed, unstandardisable state/type/status → `data/silver_enriched/_quarantine/bad_rows.jsonl` — never silently dropped |

## How to run

```bash
make enrich
# .venv/bin/python -m src.streaming.silver_enrich --silver data/silver --out data/silver_enriched
```

## Spark equivalent (needs Java 17 + Spark — documented, not run here)

```sql
-- state / claim_type standardisation + derived fields, on the Silver Delta table
SELECT
  claim_id, policy_id, customer_id,
  CASE WHEN upper(trim(state)) IN ('TX','CA',...) THEN upper(trim(state))
       WHEN upper(trim(state)) = 'TEXAS' THEN 'TX'
       -- ... full-name map, one WHEN per state; anything else -> NULL (quarantine)
       ELSE NULL END AS state,
  CASE WHEN lower(trim(claim_type)) IN ('auto','property','health','life')
         THEN lower(trim(claim_type))
       WHEN lower(trim(claim_type)) IN ('automobile','car') THEN 'auto'
       WHEN lower(trim(claim_type)) = 'home' THEN 'property'
       WHEN lower(trim(claim_type)) = 'medical' THEN 'health'
       ELSE NULL END AS claim_type,
  datediff(filed_date, incident_date) AS claim_age_days,
  claim_amount >= 25000.00 AS is_high_value
FROM silver
-- policy/customer counts: window over the Silver table itself
--   count(*) OVER (PARTITION BY customer_id) AS customer_claim_count
--   count(*) OVER (PARTITION BY policy_id)   AS policy_claim_count
-- rows with a NULL state/claim_type go to quarantine, not to enriched Silver
```

## Failure modes

- **Unknown state / type:** quarantined with the reason, not guessed.
  A new synonym belongs in `CLAIM_TYPE_SYNONYMS` / `STATE_NAMES`
  in `silver_enrich.py`, with a test.
- **Repeat counts look wrong:** they count claims *in Silver as it
  stands* — a customer with one claim in this batch is not a repeat,
  even if they claimed before the lakehouse existed. That is the
  honest scope of a derived dimension; a real master would replace it.
- **High-value threshold:** $25,000 is a demo threshold, stated in
  `HIGH_VALUE_THRESHOLD`. Change it in one place if the business
  rule changes.
