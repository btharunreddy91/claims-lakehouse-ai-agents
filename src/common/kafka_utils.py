"""Day 2 — Shared Kafka helpers (broker-free).

Single source of truth for the claims.raw topic: name, serialisation,
and record validation. Producer and consumer both import from here so
the wire format cannot drift between them.

Wire format (JSON, UTF-8):
  key   = claim_id (str) — partitions by claim, keeps one claim's
          records ordered and gives a stable partition key.
  value = Claim dict from src.common.schemas (see docs/kafka.md).
"""
from __future__ import annotations

import json

from src.common.schemas import Claim

TOPIC = "claims.raw"
DEFAULT_BOOTSTRAP = "localhost:19092"  # Redpanda external listener (docker-compose.yml)
DEFAULT_GROUP = "claims-bronze-loader"

CLAIM_KEYS = ["claim_id", "policy_id", "customer_id", "claim_type",
              "status", "claim_amount", "filed_date", "incident_date",
              "state", "description"]


def serialize_claim(claim: dict) -> bytes:
    """Claim dict -> JSON bytes for the Kafka value."""
    return json.dumps(claim, sort_keys=True).encode("utf-8")


def deserialize_claim(raw: bytes) -> dict:
    """Kafka value bytes -> Claim dict. Raises ValueError on bad JSON."""
    try:
        row = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise ValueError(f"not valid JSON: {e}") from e
    if not isinstance(row, dict):
        raise ValueError(f"expected a JSON object, got {type(row).__name__}")
    return row


def validate_record(row: dict) -> list[str]:
    """Validate a decoded claim record. Empty list = valid.

    Missing fields are treated as errors (via Claim defaults failing
    validation), never silently filled in.
    """
    try:
        claim = Claim(**{k: row.get(k, "") for k in CLAIM_KEYS})
    except Exception as e:  # wrong types, e.g. claim_amount as a string
        return [f"parse error: {e}"]
    return claim.validate()


def to_kafka_record(claim: dict) -> tuple[bytes, bytes]:
    """(key_bytes, value_bytes) for one claim dict."""
    return claim["claim_id"].encode("utf-8"), serialize_claim(claim)
