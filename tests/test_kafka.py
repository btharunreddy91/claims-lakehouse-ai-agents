"""Day 2 — broker-free Kafka tests.

No live broker required: serialisation, validation, and the
producer/consumer dry-run round-trip are all exercised against files.
"""
import json

from src.common.kafka_utils import (
    deserialize_claim,
    serialize_claim,
    to_kafka_record,
    validate_record,
)
from src.generator.kafka_producer import produce_dry_run
from src.ingestion.kafka_consumer import consume_dry_run

GOOD = {
    "claim_id": "CLM-0000001", "policy_id": "POL-12345",
    "customer_id": "CUST-1001", "claim_type": "auto", "status": "filed",
    "claim_amount": 5000.0, "filed_date": "2026-01-10",
    "incident_date": "2026-01-09", "state": "TX", "description": "x",
}


def test_serialize_deserialize_round_trip():
    key, value = to_kafka_record(GOOD)
    assert key == b"CLM-0000001"
    assert deserialize_claim(value) == GOOD
    assert serialize_claim(GOOD) == value


def test_validate_good_claim():
    assert validate_record(GOOD) == []


def test_validate_bad_claim():
    bad = {**GOOD, "claim_type": "rocket", "claim_amount": -5, "state": "ZZ"}
    errors = validate_record(bad)
    assert len(errors) >= 3
    assert any("rocket" in e for e in errors)
    assert any("claim_amount" in e for e in errors)


def test_validate_malformed_record():
    assert validate_record({"claim_id": "CLM-1"})  # missing fields -> errors
    assert validate_record({"_raw": "not json"})  # not a claim at all


def test_dry_run_round_trip(tmp_path):
    out = tmp_path / "claims_kafka.jsonl"
    n = produce_dry_run(50, seed=7, out=out)
    assert n == 50
    lines = out.read_text().splitlines()
    assert len(lines) == 50
    assert json.loads(lines[0])["key"] == "CLM-0000001"

    result = consume_dry_run(out, count=50, to_bronze=True,
                             bronze_dir=tmp_path / "bronze")
    assert result == {"read": 50, "valid": 50}
    assert (tmp_path / "bronze" / "filed_date=2026-01-10").exists() or \
        list((tmp_path / "bronze").glob("filed_date=*"))


def test_dry_run_quarantines_bad_record(tmp_path):
    out = tmp_path / "claims_kafka.jsonl"
    produce_dry_run(10, seed=7, out=out)
    # Inject one bad record at the end.
    with out.open("a") as f:
        f.write(json.dumps({"key": "CLM-BAD",
                            "value": {**GOOD, "claim_id": "CLM-BAD",
                                      "claim_type": "rocket"}}) + "\n")

    result = consume_dry_run(out, count=11, to_bronze=True,
                             bronze_dir=tmp_path / "bronze")
    assert result == {"read": 11, "valid": 10, "quarantined": 1}
    quarantine = tmp_path / "bronze" / "_quarantine" / "bad_rows.jsonl"
    assert quarantine.exists()
    assert "rocket" in quarantine.read_text()
