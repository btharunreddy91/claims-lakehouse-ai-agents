{{ config(materialized='table') }}

-- Day 5 — agg_claims_daily_state: the daily reporting mart.
-- Grain: one row per (filed_date, state, claim_type). Totals must
-- reconcile to fct_claims: sum(claim_count) = count(*) of fct_claims
-- and sum(total_claim_amount) = sum(claim_amount) of fct_claims —
-- asserted by the mirror tests in tests/test_gold.py.
select
    filed_date,
    state,
    claim_type,
    count(*) as claim_count,
    count(distinct customer_id) as customer_count,
    count(distinct policy_id) as policy_count,
    sum(claim_amount) as total_claim_amount,
    round(avg(claim_amount), 2) as avg_claim_amount,
    sum(case when is_high_value then 1 else 0 end) as high_value_claim_count
from {{ ref('stg_claims') }}
group by filed_date, state, claim_type
