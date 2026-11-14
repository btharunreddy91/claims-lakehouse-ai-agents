{{ config(materialized='table') }}

-- Day 5 — dim_policy: derived from the claims themselves, same honesty
-- rule as dim_customer (Day 4 note): no external policy master exists,
-- so first/last dates and counts come from the claims present.
select
    policy_id,
    min(filed_date) as first_filed_date,
    max(filed_date) as last_filed_date,
    count(*) as claim_count,
    sum(claim_amount) as total_claim_amount,
    sum(case when is_high_value then 1 else 0 end) as high_value_claim_count
from {{ ref('stg_claims') }}
group by policy_id
