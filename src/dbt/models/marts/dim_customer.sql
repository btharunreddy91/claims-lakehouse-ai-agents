{{ config(materialized='table') }}

-- Day 5 — dim_customer: derived from the claims themselves.
-- There is no external customer master in this demo (Day 4 note), so the
-- name is the first non-empty name seen for the id and every count is
-- computed from the claims present — nothing invented.
select
    customer_id,
    arg_max(customer_name, filed_date) as customer_name,
    min(filed_date) as first_filed_date,
    max(filed_date) as last_filed_date,
    count(*) as claim_count,
    sum(claim_amount) as total_claim_amount,
    sum(case when is_high_value then 1 else 0 end) as high_value_claim_count
from {{ ref('stg_claims') }}
group by customer_id
