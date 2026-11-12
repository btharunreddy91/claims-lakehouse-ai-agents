"""Day 3 — Silver transform tests (Spark-free mirror).

No Java / Spark required: these exercise the exact rules the
Structured Streaming job applies, via src.streaming.silver_transform.
"""
import json

from src.streaming.silver_transform import (
    load_bronze,
    transform_rows,
    write_silver,
)

GOOD = {
    "claim_id": "CLM-0000001", "policy_id": "POL-12345",
    "customer_id": "CUST-1001", "claim_type": "auto", "status": "filed",
    "claim_amount": 5000.0, "filed_date": "2026-03-10",
    "incident_date": "2026-03-09", "state": "TX",
    "description": "auto claim", "ingested_at": "2026-03-10T12:00:00+00:00",
}


def test_casts_amount_and_dates():
    row = dict(GOOD, claim_amount="5000.50")  # string amount, as Bronze may carry
    out = transform_rows([row])
    assert out["counts"]["silver"] == 1
    silver = out["silver"][0]
    assert isinstance(silver["claim_amount"], float)
    assert silver["claim_amount"] == 5000.50
    assert silver["filed_date"] == "2026-03-10"
    assert silver["incident_date"] == "2026-03-09"


def test_dedupe_keeps_latest_ingested():
    old = dict(GOOD, claim_amount=100.0, ingested_at="2026-03-10T10:00:00+00:00")
    new = dict(GOOD, claim_amount=900.0, ingested_at="2026-03-10T12:00:00+00:00")
    out = transform_rows([old, new])
    assert out["counts"]["silver"] == 1
    assert out["counts"]["duplicates_dropped"] == 1
    assert out["silver"][0]["claim_amount"] == 900.0


def test_dedupe_is_order_independent():
    old = dict(GOOD, claim_amount=100.0, ingested_at="2026-03-10T10:00:00+00:00")
    new = dict(GOOD, claim_amount=900.0, ingested_at="2026-03-10T12:00:00+00:00")
    out = transform_rows([new, old])  # newest first — same winner
    assert out["silver"][0]["claim_amount"] == 900.0


def test_watermark_drops_late_row():
    recent = dict(GOOD, claim_id="CLM-1", filed_date="2026-03-10",
                  incident_date="2026-03-09")
    late = dict(GOOD, claim_id="CLM-2", filed_date="2026-03-01",
                incident_date="2026-02-28")  # 9 days before max -> late
    out = transform_rows([recent, late], watermark_days=7)
    assert out["counts"]["silver"] == 1
    assert out["counts"]["late_dropped"] == 1
    assert out["silver"][0]["claim_id"] == "CLM-1"
    assert out["late"][0]["claim_id"] == "CLM-2"


def test_watermark_boundary_kept():
    recent = dict(GOOD, claim_id="CLM-1", filed_date="2026-03-10",
                  incident_date="2026-03-09")
    edge = dict(GOOD, claim_id="CLM-2", filed_date="2026-03-03",
                incident_date="2026-03-02")  # exactly 7 days before max
    out = transform_rows([recent, edge], watermark_days=7)
    assert out["counts"]["silver"] == 2
    assert out["counts"]["late_dropped"] == 0


def test_invalid_rows_quarantined_not_dropped():
    bad_type = dict(GOOD, claim_id="CLM-BAD1", claim_type="mystery")
    bad_amount = dict(GOOD, claim_id="CLM-BAD2", claim_amount=-50.0)
    bad_dates = dict(GOOD, claim_id="CLM-BAD3", filed_date="2026-03-09",
                     incident_date="2026-03-10")  # incident after filed
    out = transform_rows([GOOD, bad_type, bad_amount, bad_dates])
    assert out["counts"]["silver"] == 1
    assert out["counts"]["quarantined"] == 3
    assert {b["row"]["claim_id"] for b in out["quarantined"]} == {
        "CLM-BAD1", "CLM-BAD2", "CLM-BAD3"}
    assert all(b["errors"] for b in out["quarantined"])


def test_write_silver_partitions_by_filed_date(tmp_path):
    rows = [dict(GOOD, claim_id="CLM-1"),
            dict(GOOD, claim_id="CLM-2", filed_date="2026-03-11",
                 incident_date="2026-03-10")]
    out = transform_rows(rows)
    written = write_silver(out["silver"], tmp_path / "silver")
    assert written == {"partitions": 2, "rows": 2}
    part = tmp_path / "silver" / "filed_date=2026-03-10" / "part.jsonl"
    assert json.loads(part.read_text().splitlines()[0])["claim_id"] == "CLM-1"


def test_load_bronze_skips_quarantine(tmp_path):
    bronze = tmp_path / "bronze"
    (bronze / "filed_date=2026-03-10").mkdir(parents=True)
    (bronze / "filed_date=2026-03-10" / "part.jsonl").write_text(
        json.dumps(GOOD) + "\n")
    (bronze / "_quarantine").mkdir()
    (bronze / "_quarantine" / "bad_rows.jsonl").write_text("{}\n")
    assert len(load_bronze(bronze)) == 1
