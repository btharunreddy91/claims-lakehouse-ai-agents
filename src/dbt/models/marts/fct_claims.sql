{{ config(materialized='table') }}

-- Day 5 — fct_claims: one row per claim, the grain every mart joins to.
-- Enriched Silver is already deduplicated by claim_id (Day 3), so this
-- is a pass-through of stg_claims with the grain asserted by the
-- unique/not_null tests on claim_id in schema.yml.
select
    claim_id,
    policy_id,
    customer_id,
    claim_type,
    status,
    claim_amount,
    filed_date,
    incident_date,
    state,
    claim_age_days,
    is_high_value,
    customer_claim_count,
    policy_claim_count,
    description,
    ingested_at
from {{ ref('stg_claims') }}
