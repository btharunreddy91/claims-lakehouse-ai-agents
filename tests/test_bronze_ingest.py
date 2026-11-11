import json
from pathlib import Path

from src.ingestion.bronze_ingest import ingest


def test_ingest_valid_and_quarantine(tmp_path):
    raw = tmp_path / "claims.jsonl"
    raw.write_text("\n".join([
        json.dumps({"claim_id": "CLM-1", "policy_id": "POL-1", "customer_id": "CUST-1",
                    "claim_type": "auto", "status": "filed", "claim_amount": 5000,
                    "filed_date": "2026-01-10", "incident_date": "2026-01-09",
                    "state": "TX", "description": "x"}),
        json.dumps({"claim_id": "CLM-2", "policy_id": "POL-2", "customer_id": "CUST-2",
                    "claim_type": "rocket", "status": "filed", "claim_amount": -5,
                    "filed_date": "2026-01-10", "incident_date": "2026-01-09",
                    "state": "TX", "description": "bad"}),
    ]))
    result = ingest(raw, tmp_path / "bronze")
    assert result == {"read": 2, "valid": 1, "quarantined": 1}
    assert (tmp_path / "bronze" / "filed_date=2026-01-10" / "part.jsonl").exists()
    assert (tmp_path / "bronze" / "_quarantine" / "bad_rows.jsonl").exists()
