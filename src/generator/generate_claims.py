"""Day 1 — Synthetic claims generator.

Generates realistic insurance claims (no real PII) and writes them as JSONL,
partitioned by filed_date, ready for Bronze ingestion.

Usage:
    python -m src.generator.generate_claims --count 1000
    python -m src.generator.generate_claims --count 100 --out data/raw/claims.jsonl --seed 42
"""
from __future__ import annotations

import argparse
import json
import random
from datetime import date, timedelta
from pathlib import Path

from src.common.schemas import CLAIM_STATUSES, CLAIM_TYPES, US_STATES

FIRST = ["Aarav", "Priya", "Marcus", "Elena", "James", "Sofia", "David", "Meera",
         "Chris", "Anita", "Robert", "Lena", "Kiran", "Sarah", "Tom", "Nina"]
LAST = ["Sharma", "Patel", "Johnson", "Garcia", "Kim", "Nguyen", "Smith", "Reddy",
        "Chen", "Brown", "Wilson", "Davis", "Khan", "Lee", "Moore", "Ali"]

# Amount ranges by claim type (mean, sd) — auto/property skew higher.
AMOUNT = {"auto": (8500, 6000), "property": (22000, 18000),
          "health": (3200, 2500), "life": (45000, 30000)}


def _claim(rng: random.Random, i: int) -> dict:
    claim_type = rng.choices(CLAIM_TYPES, weights=[0.38, 0.27, 0.25, 0.10])[0]
    mean, sd = AMOUNT[claim_type]
    amount = round(max(100.0, rng.gauss(mean, sd)), 2)
    incident = date(2026, 1, 1) + timedelta(days=rng.randrange(0, 310))
    filed = incident + timedelta(days=rng.randrange(0, 14))
    return {
        "claim_id": f"CLM-{i:07d}",
        "policy_id": f"POL-{rng.randrange(10000, 99999)}",
        "customer_id": f"CUST-{rng.randrange(1000, 9999)}",
        "customer_name": f"{rng.choice(FIRST)} {rng.choice(LAST)}",  # PII — masked in Gold
        "claim_type": claim_type,
        "status": rng.choices(CLAIM_STATUSES, weights=[0.22, 0.28, 0.18, 0.08, 0.14, 0.10])[0],
        "claim_amount": amount,
        "filed_date": filed.isoformat(),
        "incident_date": incident.isoformat(),
        "state": rng.choices(US_STATES, weights=[0.22, 0.16, 0.12, 0.08, 0.12, 0.08, 0.07, 0.06, 0.05, 0.04])[0],
        "description": f"{claim_type} claim filed in {filed.year}",
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--count", type=int, default=1000)
    ap.add_argument("--out", default="data/raw/claims.jsonl")
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w") as f:
        for i in range(1, args.count + 1):
            f.write(json.dumps(_claim(rng, i)) + "\n")
    print(f"Wrote {args.count} claims → {out}")


if __name__ == "__main__":
    main()
