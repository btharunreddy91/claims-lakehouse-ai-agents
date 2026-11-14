{{ config(materialized='view') }}

-- Day 5 — stg_claims: thin typed layer over enriched Silver.
-- No business logic here beyond casting; every rule lives in the mart
-- models so it can be tested once, in one place.
select
    claim_id,
    policy_id,
    customer_id,
    customer_name,
    lower(trim(claim_type)) as claim_type,
    lower(trim(status)) as status,
    cast(claim_amount as double) as claim_amount,
    cast(filed_date as date) as filed_date,
    cast(incident_date as date) as incident_date,
    upper(trim(state)) as state,
    description,
    cast(claim_age_days as integer) as claim_age_days,
    cast(is_high_value as boolean) as is_high_value,
    cast(customer_claim_count as integer) as customer_claim_count,
    cast(policy_claim_count as integer) as policy_claim_count,
    cast(ingested_at as timestamp) as ingested_at
from {{ source('silver', 'silver_enriched') }}
