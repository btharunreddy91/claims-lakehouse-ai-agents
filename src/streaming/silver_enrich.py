"""Day 4 — Silver cleaning + enrichment (Spark-free mirror).

Takes Day 3 Silver (``data/silver``) and produces enriched Silver
(``data/silver_enriched``), partitioned by ``filed_date``:

1. Standardise ``state`` — strip/upper-case, map full names
   ("Texas", "texas") to the 2-letter code. Unknown values are
   quarantined, never silently fixed or dropped.
2. Standardise ``claim_type`` — lower/strip, map synonyms
   ("automobile"/"car" -> "auto", "home" -> "property",
   "medical" -> "health"). Unknown values are quarantined.
3. Derive ``claim_age_days`` = (filed_date - incident_date).days
   and ``is_high_value`` = claim_amount >= 25,000.00.
4. Policy / customer join — there is no external policy or customer
   master in this demo (the source systems supply only IDs), so the
   dimensions are derived from Silver itself: ``dim_customer`` and
   ``dim_policy`` built from first-seen claims, and each enriched row
   carries ``customer_claim_count``, ``policy_claim_count``,
   ``is_repeat_customer`` and ``is_repeat_policy``. Day 5 (dbt)
   formalises these as ``dim_customer`` / ``dim_policy``. Nothing
   about a policy or customer is invented here — counts are computed
   from the claims actually present.
5. Quarantine — rows with a missing policy/customer/claim id, an
   unparseable amount or date, or an unstandardisable state/type
   go to ``data/silver_enriched/_quarantine/bad_rows.jsonl``.

The Spark equivalent is the SQL in ``docs/silver_enrichment.md``
(``upper()``, ``datediff()``, ``case when`` + a self-join for the
counts); it needs Java + Spark, so it is documented, not run here.

Usage:
    python -m src.streaming.silver_enrich --silver data/silver --out data/silver_enriched
    # or: make enrich
"""
from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import date
from pathlib import Path

from src.common.schemas import CLAIM_STATUSES, CLAIM_TYPES, US_STATES

HIGH_VALUE_THRESHOLD = 25_000.00

STATE_NAMES = {
    "ALABAMA": "AL", "ALASKA": "AK", "ARIZONA": "AZ", "ARKANSAS": "AR",
    "CALIFORNIA": "CA", "COLORADO": "CO", "CONNECTICUT": "CT",
    "DELAWARE": "DE", "FLORIDA": "FL", "GEORGIA": "GA", "HAWAII": "HI",
    "IDAHO": "ID", "ILLINOIS": "IL", "INDIANA": "IN", "IOWA": "IA",
    "KANSAS": "KS", "KENTUCKY": "KY", "LOUISIANA": "LA", "MAINE": "ME",
    "MARYLAND": "MD", "MASSACHUSETTS": "MA", "MICHIGAN": "MI",
    "MINNESOTA": "MN", "MISSISSIPPI": "MS", "MISSOURI": "MO",
    "MONTANA": "MT", "NEBRASKA": "NE", "NEVADA": "NV",
    "NEW HAMPSHIRE": "NH", "NEW JERSEY": "NJ", "NEW MEXICO": "NM",
    "NEW YORK": "NY", "NORTH CAROLINA": "NC", "NORTH DAKOTA": "ND",
    "OHIO": "OH", "OKLAHOMA": "OK", "OREGON": "OR", "PENNSYLVANIA": "PA",
    "RHODE ISLAND": "RI", "SOUTH CAROLINA": "SC", "SOUTH DAKOTA": "SD",
    "TENNESSEE": "TN", "TEXAS": "TX", "UTAH": "UT", "VERMONT": "VT",
    "VIRGINIA": "VA", "WASHINGTON": "WA", "WEST VIRGINIA": "WV",
    "WISCONSIN": "WI", "WYOMING": "WY",
    "DISTRICT OF COLUMBIA": "DC",
}

CLAIM_TYPE_SYNONYMS = {
    "automobile": "auto", "car": "auto", "auto insurance": "auto",
    "vehicle": "auto",
    "home": "property", "homeowners": "property", "homeowner": "property",
    "property damage": "property",
    "medical": "health", "health insurance": "health",
    "life insurance": "life",
}


def standardise_state(raw) -> str | None:
    """Return the 2-letter state code, or None if it cannot be resolved."""
    if raw is None:
        return None
    s = str(raw).strip().upper()
    if s in US_STATES:
        return s
    return STATE_NAMES.get(s)


def standardise_claim_type(raw) -> str | None:
    """Return the canonical claim type, or None if it cannot be resolved."""
    if raw is None:
        return None
    s = str(raw).strip().lower()
    if s in CLAIM_TYPES:
        return s
    return CLAIM_TYPE_SYNONYMS.get(s)


def build_dimensions(rows: list[dict]) -> tuple[dict, dict]:
    """Derive dim_customer / dim_policy from the claims themselves.

    Returns (dim_customer, dim_policy) keyed by id, each with the
    first-seen name (customer only), first/last filed dates and the
    claim count. Counts are computed over the *input* rows, so a
    repeat is a repeat in the data at hand — not an external fact.
    """
    dim_customer: dict[str, dict] = {}
    dim_policy: dict[str, dict] = {}
    for row in rows:
        cid, pid = str(row.get("customer_id", "")), str(row.get("policy_id", ""))
        filed = str(row.get("filed_date", ""))
        if cid:
            d = dim_customer.setdefault(cid, {
                "customer_id": cid,
                "customer_name": row.get("customer_name", ""),
                "first_filed_date": filed, "last_filed_date": filed,
                "claim_count": 0,
            })
            d["claim_count"] += 1
            if filed:
                d["first_filed_date"] = min(d["first_filed_date"], filed)
                d["last_filed_date"] = max(d["last_filed_date"], filed)
        if pid:
            d = dim_policy.setdefault(pid, {
                "policy_id": pid,
                "first_filed_date": filed, "last_filed_date": filed,
                "claim_count": 0,
            })
            d["claim_count"] += 1
            if filed:
                d["first_filed_date"] = min(d["first_filed_date"], filed)
                d["last_filed_date"] = max(d["last_filed_date"], filed)
    return dim_customer, dim_policy


def enrich_rows(rows: list[dict]) -> dict:
    """Apply Day 4 cleaning + enrichment to Silver rows.

    Returns {"enriched": [...], "quarantined": [...],
    "dim_customer": {...}, "dim_policy": {...}, "counts": {...}}.
    """
    counts: Counter = Counter()
    dim_customer, dim_policy = build_dimensions(rows)
    enriched: list[dict] = []
    quarantined: list[dict] = []

    for row in rows:
        counts["read"] += 1
        errors: list[str] = []
        claim_id = str(row.get("claim_id", "")).strip()
        policy_id = str(row.get("policy_id", "")).strip()
        customer_id = str(row.get("customer_id", "")).strip()
        if not claim_id:
            errors.append("missing claim_id")
        if not policy_id:
            errors.append("missing policy_id")
        if not customer_id:
            errors.append("missing customer_id")

        state = standardise_state(row.get("state"))
        if state is None:
            errors.append(f"state={row.get('state')!r} cannot be standardised")
        claim_type = standardise_claim_type(row.get("claim_type"))
        if claim_type is None:
            errors.append(f"claim_type={row.get('claim_type')!r} cannot be standardised")
        status = str(row.get("status", "")).strip().lower()
        if status not in CLAIM_STATUSES:
            errors.append(f"status={row.get('status')!r} not in {CLAIM_STATUSES}")

        try:
            amount = float(row["claim_amount"])
        except (KeyError, TypeError, ValueError):
            errors.append(f"claim_amount={row.get('claim_amount')!r} is not a number")
            amount = 0.0
        else:
            if amount < 0:
                errors.append(f"claim_amount={amount!r} must be >= 0")

        try:
            filed = date.fromisoformat(str(row["filed_date"]))
            incident = date.fromisoformat(str(row["incident_date"]))
        except (KeyError, ValueError) as e:
            errors.append(f"bad date: {e}")
            filed = incident = None
        else:
            if incident > filed:
                errors.append("incident_date after filed_date")

        if errors:
            counts["quarantined"] += 1
            quarantined.append({"row": row, "errors": errors})
            continue

        cust = dim_customer[customer_id]
        pol = dim_policy[policy_id]
        out = dict(row)
        out.update({
            "state": state,
            "claim_type": claim_type,
            "status": status,
            "claim_amount": amount,
            "filed_date": filed.isoformat(),
            "incident_date": incident.isoformat(),
            "claim_age_days": (filed - incident).days,
            "is_high_value": amount >= HIGH_VALUE_THRESHOLD,
            "customer_claim_count": cust["claim_count"],
            "policy_claim_count": pol["claim_count"],
            "is_repeat_customer": cust["claim_count"] > 1,
            "is_repeat_policy": pol["claim_count"] > 1,
        })
        enriched.append(out)
        counts["enriched"] += 1
        if out["is_high_value"]:
            counts["high_value"] += 1
        if out["is_repeat_customer"]:
            counts["repeat_customer_rows"] += 1

    counts["dim_customers"] = len(dim_customer)
    counts["dim_policies"] = len(dim_policy)
    return {"enriched": enriched, "quarantined": quarantined,
            "dim_customer": dim_customer, "dim_policy": dim_policy,
            "counts": dict(counts)}


def load_silver(silver_dir: Path) -> list[dict]:
    """Read all Silver part.jsonl files (skips _quarantine / _late)."""
    rows: list[dict] = []
    for part in sorted(silver_dir.glob("filed_date=*/part.jsonl")):
        for line in part.read_text().splitlines():
            if line.strip():
                rows.append(json.loads(line))
    return rows


def write_enriched(enriched: list[dict], out_dir: Path) -> dict:
    """Write enriched rows partitioned by filed_date (JSONL)."""
    by_date: dict[str, list[dict]] = {}
    for row in enriched:
        by_date.setdefault(row["filed_date"], []).append(row)
    for filed_date, rows in by_date.items():
        part = out_dir / f"filed_date={filed_date}"
        part.mkdir(parents=True, exist_ok=True)
        with (part / "part.jsonl").open("w") as f:
            for r in rows:
                f.write(json.dumps(r) + "\n")
    return {"partitions": len(by_date), "rows": len(enriched)}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--silver", default="data/silver")
    ap.add_argument("--out", default="data/silver_enriched")
    args = ap.parse_args()

    silver_dir, out_dir = Path(args.silver), Path(args.out)
    rows = load_silver(silver_dir)
    result = enrich_rows(rows)
    written = write_enriched(result["enriched"], out_dir)
    if result["quarantined"]:
        q = out_dir / "_quarantine"
        q.mkdir(parents=True, exist_ok=True)
        with (q / "bad_rows.jsonl").open("w") as f:
            for b in result["quarantined"]:
                f.write(json.dumps(b) + "\n")
    print(f"Day 4 enrich: read={result['counts']['read']} "
          f"enriched={result['counts']['enriched']} "
          f"quarantined={result['counts'].get('quarantined', 0)} "
          f"high_value={result['counts'].get('high_value', 0)} "
          f"dim_customers={result['counts']['dim_customers']} "
          f"dim_policies={result['counts']['dim_policies']} "
          f"-> {written}")


if __name__ == "__main__":
    main()
