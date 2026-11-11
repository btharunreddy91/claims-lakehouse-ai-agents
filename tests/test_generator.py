from src.common.schemas import Claim
from src.generator.generate_claims import _claim
import random


def test_generated_claim_is_valid():
    rng = random.Random(1)
    for i in range(1, 200):
        row = _claim(rng, i)
        claim = Claim(**{k: row[k] for k in
                         ["claim_id", "policy_id", "customer_id", "claim_type",
                          "status", "claim_amount", "filed_date", "incident_date",
                          "state", "description"]})
        assert claim.validate() == [], claim.validate()


def test_amounts_are_positive():
    rng = random.Random(2)
    assert all(_claim(rng, i)["claim_amount"] > 0 for i in range(50))
