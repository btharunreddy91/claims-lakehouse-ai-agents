"""Shared schemas — single source of truth for claims data.

Day 1: plain dataclasses + validation, no Spark dependency.
Spark / Delta schemas (Day 3) are derived from these field lists.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone

CLAIM_TYPES = ["auto", "property", "health", "life"]
CLAIM_STATUSES = ["filed", "in_review", "approved", "denied", "paid", "closed"]
US_STATES = ["TX", "CA", "NY", "AZ", "FL", "IL", "GA", "NC", "OH", "WA"]

# Bronze = raw, as received. All fields nullable except claim_id / event time.
CLAIM_FIELDS = [
    "claim_id", "policy_id", "customer_id", "claim_type", "status",
    "claim_amount", "filed_date", "incident_date", "state",
    "description", "ingested_at",
]

REQUIRED_FIELDS = ["claim_id", "policy_id", "customer_id", "claim_type", "claim_amount"]


@dataclass
class Claim:
    claim_id: str
    policy_id: str
    customer_id: str
    claim_type: str
    status: str
    claim_amount: float
    filed_date: str  # YYYY-MM-DD
    incident_date: str  # YYYY-MM-DD
    state: str
    description: str = ""
    ingested_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def validate(self) -> list[str]:
        """Return a list of validation errors; empty = valid."""
        errors: list[str] = []
        if self.claim_type not in CLAIM_TYPES:
            errors.append(f"claim_type={self.claim_type!r} not in {CLAIM_TYPES}")
        if self.status not in CLAIM_STATUSES:
            errors.append(f"status={self.status!r} not in {CLAIM_STATUSES}")
        if self.state not in US_STATES:
            errors.append(f"state={self.state!r} not in {US_STATES}")
        try:
            amount = float(self.claim_amount)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            errors.append(f"claim_amount={self.claim_amount!r} is not a number")
        else:
            if amount < 0:
                errors.append(f"claim_amount={self.claim_amount!r} must be >= 0")
        try:
            filed = date.fromisoformat(self.filed_date)
            incident = date.fromisoformat(self.incident_date)
        except ValueError as e:
            errors.append(f"bad date: {e}")
        else:
            if incident > filed:
                errors.append("incident_date after filed_date")
        return errors
