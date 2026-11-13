from src.common.schemas import Claim
from src.ingestion.load_open_data import map_row

ROW = {
    "policy_number": "521585", "policy_state": "OH",
    "incident_date": "2015-01-25", "incident_type": "Single Vehicle Collision",
    "incident_severity": "Major Damage", "fraud_reported": "Y",
    "total_claim_amount": "71610", "auto_make": "Saab",
    "auto_model": "92x", "auto_year": "2004",
}


def test_mapping_is_valid_claim():
    mapped = map_row(ROW, 1)
    assert mapped["claim_id"] == "OPEN-0001"
    assert mapped["claim_type"] == "auto"
    assert mapped["claim_amount"] == 71610.0
    assert mapped["state"] == "OH"
    claim = Claim(**{k: mapped[k] for k in
                     ["claim_id", "policy_id", "customer_id", "claim_type",
                      "status", "claim_amount", "filed_date", "incident_date",
                      "state", "description"]})
    assert claim.validate() == []


def test_fraud_is_kept_in_description_not_status():
    mapped = map_row(ROW, 1)
    assert mapped["status"] == "filed"
    assert "fraud_reported=Y" in mapped["description"]


def test_indiana_state_is_now_valid():
    row = dict(ROW, policy_state="IN")
    mapped = map_row(row, 2)
    claim = Claim(**{k: mapped[k] for k in
                     ["claim_id", "policy_id", "customer_id", "claim_type",
                      "status", "claim_amount", "filed_date", "incident_date",
                      "state", "description"]})
    assert claim.validate() == []
