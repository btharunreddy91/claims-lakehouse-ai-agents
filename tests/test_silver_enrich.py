"""Day 4 — Silver cleaning + enrichment tests (Spark-free mirror)."""
import json

from src.streaming.silver_enrich import (
    build_dimensions,
    enrich_rows,
    load_silver,
    standardise_claim_type,
    standardise_state,
    write_enriched,
)

GOOD = {
    "claim_id": "CLM-0000001", "policy_id": "POL-12345",
    "customer_id": "CUST-1001", "customer_name": "Priya Sharma",
    "claim_type": "auto", "status": "filed",
    "claim_amount": 5000.0, "filed_date": "2026-03-10",
    "incident_date": "2026-03-09", "state": "TX",
    "description": "auto claim", "ingested_at": "2026-03-10T12:00:00+00:00",
}


def test_standardises_state_full_name_and_case():
    assert standardise_state("Texas") == "TX"
    assert standardise_state("texas") == "TX"
    assert standardise_state(" tx ") == "TX"
    assert standardise_state("New York") == "NY"
    assert standardise_state("XX") is None
    assert standardise_state("") is None


def test_standardises_claim_type_synonyms():
    assert standardise_claim_type("Automobile") == "auto"
    assert standardise_claim_type("car") == "auto"
    assert standardise_claim_type("home") == "property"
    assert standardise_claim_type("medical") == "health"
    assert standardise_claim_type("life insurance") == "life"
    assert standardise_claim_type("mystery") is None


def test_derives_age_and_high_value():
    row = dict(GOOD, claim_amount=30_000.0)
    out = enrich_rows([row])
    assert out["counts"]["enriched"] == 1
    silver = out["enriched"][0]
    assert silver["claim_age_days"] == 1  # filed Mar 10, incident Mar 9
    assert silver["is_high_value"] is True


def test_high_value_boundary():
    at = enrich_rows([dict(GOOD, claim_amount=25_000.0)])
    assert at["enriched"][0]["is_high_value"] is True
    below = enrich_rows([dict(GOOD, claim_amount=24_999.99)])
    assert below["enriched"][0]["is_high_value"] is False


def test_standardisation_applied_in_row():
    row = dict(GOOD, state="texas", claim_type="Automobile")
    out = enrich_rows([row])
    assert out["enriched"][0]["state"] == "TX"
    assert out["enriched"][0]["claim_type"] == "auto"


def test_unknown_state_or_type_quarantined_not_dropped():
    bad_state = dict(GOOD, claim_id="CLM-BAD1", state="XX")
    bad_type = dict(GOOD, claim_id="CLM-BAD2", claim_type="mystery")
    out = enrich_rows([GOOD, bad_state, bad_type])
    assert out["counts"]["enriched"] == 1
    assert out["counts"]["quarantined"] == 2
    assert {b["row"]["claim_id"] for b in out["quarantined"]} == {"CLM-BAD1", "CLM-BAD2"}
    assert all(b["errors"] for b in out["quarantined"])


def test_missing_policy_or_customer_quarantined():
    no_policy = dict(GOOD, claim_id="CLM-BAD3", policy_id="")
    no_customer = dict(GOOD, claim_id="CLM-BAD4", customer_id="")
    out = enrich_rows([no_policy, no_customer])
    assert out["counts"]["quarantined"] == 2


def test_repeat_counts_come_from_the_data():
    first = dict(GOOD, claim_id="CLM-1")
    second = dict(GOOD, claim_id="CLM-2")  # same customer + policy
    third = dict(GOOD, claim_id="CLM-3", customer_id="CUST-9999",
                 policy_id="POL-99999")
    out = enrich_rows([first, second, third])
    assert out["counts"]["enriched"] == 3
    by_id = {r["claim_id"]: r for r in out["enriched"]}
    assert by_id["CLM-1"]["customer_claim_count"] == 2
    assert by_id["CLM-1"]["policy_claim_count"] == 2
    assert by_id["CLM-1"]["is_repeat_customer"] is True
    assert by_id["CLM-3"]["customer_claim_count"] == 1
    assert by_id["CLM-3"]["is_repeat_customer"] is False
    assert out["counts"]["dim_customers"] == 2
    assert out["counts"]["dim_policies"] == 2


def test_dimensions_carry_first_seen_name_and_dates():
    rows = [dict(GOOD, claim_id="CLM-1", filed_date="2026-03-10",
                 incident_date="2026-03-09"),
            dict(GOOD, claim_id="CLM-2", filed_date="2026-03-12",
                 incident_date="2026-03-11")]
    dim_customer, dim_policy = build_dimensions(rows)
    cust = dim_customer["CUST-1001"]
    assert cust["customer_name"] == "Priya Sharma"
    assert cust["first_filed_date"] == "2026-03-10"
    assert cust["last_filed_date"] == "2026-03-12"
    assert cust["claim_count"] == 2
    assert dim_policy["POL-12345"]["claim_count"] == 2


def test_write_enriched_partitions_by_filed_date(tmp_path):
    rows = [dict(GOOD, claim_id="CLM-1"),
            dict(GOOD, claim_id="CLM-2", filed_date="2026-03-11",
                 incident_date="2026-03-10")]
    out = enrich_rows(rows)
    written = write_enriched(out["enriched"], tmp_path / "enriched")
    assert written == {"partitions": 2, "rows": 2}
    part = tmp_path / "enriched" / "filed_date=2026-03-10" / "part.jsonl"
    row = json.loads(part.read_text().splitlines()[0])
    assert row["claim_id"] == "CLM-1"
    assert row["claim_age_days"] == 1


def test_load_silver_skips_quarantine_and_late(tmp_path):
    silver = tmp_path / "silver"
    (silver / "filed_date=2026-03-10").mkdir(parents=True)
    (silver / "filed_date=2026-03-10" / "part.jsonl").write_text(
        json.dumps(GOOD) + "\n")
    (silver / "_quarantine").mkdir()
    (silver / "_quarantine" / "bad_rows.jsonl").write_text("{}\n")
    (silver / "_late").mkdir()
    (silver / "_late" / "late_rows.jsonl").write_text("{}\n")
    assert len(load_silver(silver)) == 1
