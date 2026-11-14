"""Day 5 — dbt Gold marts tests (stdlib mirror).

Exercises the exact rules the dbt models declare, via
src/dbt/build_gold, plus a reconciliation check over the real
enriched Silver: agg totals must equal fct totals.
"""
import json
from pathlib import Path

import pytest

from src.dbt.build_gold import (
    build_agg,
    build_dim_customer,
    build_dim_policy,
    build_fct,
    build_gold,
    build_stg,
)

GOOD = {
    "claim_id": "CLM-0000001", "policy_id": "POL-12345",
    "customer_id": "CUST-1001", "customer_name": "Priya Sharma",
    "claim_type": "auto", "status": "filed",
    "claim_amount": 5000.0, "filed_date": "2026-03-10",
    "incident_date": "2026-03-09", "state": "TX",
    "description": "auto claim", "ingested_at": "2026-03-10T12:00:00+00:00",
    "claim_age_days": 1, "is_high_value": False,
    "customer_claim_count": 1, "policy_claim_count": 1,
}


def _stg(*rows):
    return build_stg([dict(r) for r in rows])


def test_stg_casts_and_normalises():
    row = dict(GOOD, claim_amount="5000.50", claim_type=" Auto ", state="tx")
    out = _stg(row)
    assert out[0]["claim_amount"] == 5000.50
    assert isinstance(out[0]["claim_amount"], float)
    assert out[0]["claim_type"] == "auto"
    assert out[0]["state"] == "TX"


def test_fct_dedupe_keeps_latest_ingested():
    old = dict(GOOD, claim_amount=100.0, ingested_at="2026-03-10T10:00:00+00:00")
    new = dict(GOOD, claim_amount=900.0, ingested_at="2026-03-10T12:00:00+00:00")
    fct = build_fct(_stg(old, new))
    assert len(fct) == 1
    assert fct[0]["claim_amount"] == 900.0


def test_dim_customer_first_seen_name_and_counts():
    r1 = dict(GOOD, claim_id="CLM-1", filed_date="2026-03-10", claim_amount=100.0)
    r2 = dict(GOOD, claim_id="CLM-2", filed_date="2026-03-12", claim_amount=30000.0,
              is_high_value=True)
    dims = build_dim_customer(build_fct(_stg(r1, r2)))
    assert len(dims) == 1
    d = dims[0]
    assert d["customer_name"] == "Priya Sharma"
    assert d["first_filed_date"] == "2026-03-10"
    assert d["last_filed_date"] == "2026-03-12"
    assert d["claim_count"] == 2
    assert d["total_claim_amount"] == 30100.0
    assert d["high_value_claim_count"] == 1


def test_dim_policy_groups_by_policy():
    r1 = dict(GOOD, claim_id="CLM-1", claim_amount=100.0)
    r2 = dict(GOOD, claim_id="CLM-2", policy_id="POL-99999", claim_amount=200.0)
    dims = build_dim_policy(build_fct(_stg(r1, r2)))
    assert len(dims) == 2
    by_id = {d["policy_id"]: d for d in dims}
    assert by_id["POL-12345"]["claim_count"] == 1
    assert by_id["POL-99999"]["total_claim_amount"] == 200.0


def test_agg_groups_and_averages():
    r1 = dict(GOOD, claim_id="CLM-1", claim_amount=100.0)
    r2 = dict(GOOD, claim_id="CLM-2", claim_amount=300.0)
    r3 = dict(GOOD, claim_id="CLM-3", state="CA", claim_amount=50.0)
    agg = build_agg(build_fct(_stg(r1, r2, r3)))
    assert len(agg) == 2
    tx = [a for a in agg if a["state"] == "TX"][0]
    assert tx["claim_count"] == 2
    assert tx["total_claim_amount"] == 400.0
    assert tx["avg_claim_amount"] == 200.0
    assert tx["customer_count"] == 1
    assert tx["policy_count"] == 1


def test_gold_reconciles_over_real_silver(tmp_path):
    repo = Path(__file__).resolve().parents[1]
    manifest = build_gold(repo / "data" / "silver_enriched", tmp_path / "gold")
    assert manifest["source_rows"] == manifest["fct_claims"] == 758
    assert manifest["agg_claim_count"] == manifest["fct_claims"]
    assert manifest["agg_total_claim_amount"] == manifest["fct_total_claim_amount"]
    assert manifest["dim_customer"] == 725  # Day 4 count, derived from Silver
    assert manifest["dim_policy"] == 755
    assert manifest["high_value_claims"] == 166  # Day 4 count (>= $25k)
    # Every dim row's claim_count sums back to the fact count.
    dim_c = [json.loads(l) for l in (tmp_path / "gold" / "dim_customer.jsonl").read_text().splitlines()]
    assert sum(d["claim_count"] for d in dim_c) == 758
    dim_p = [json.loads(l) for l in (tmp_path / "gold" / "dim_policy.jsonl").read_text().splitlines()]
    assert sum(d["claim_count"] for d in dim_p) == 758
    assert len({d["customer_id"] for d in dim_c}) == len(dim_c)
    assert len({d["policy_id"] for d in dim_p}) == len(dim_p)
