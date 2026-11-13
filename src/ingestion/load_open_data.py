"""Map open-source insurance claims to the Claim schema.

Source: Aqqad (2023), insurance_claims, Mendeley Data, DOI 10.17632/992mh7dk9y.2
— 1,000 anonymised auto-insurance claims, 40 columns.
File: data/raw_open/insurance_claims.csv (see docs/open_data.md for provenance).

Mapping — honest about what the source does NOT have:
- claim_id: OPEN-0001 … OPEN-1000 (source has no claim id; policy_number is
  the policy, not the claim)
- policy_id / customer_id: derived from policy_number (source is anonymised;
  no customer id is given, so customer is keyed to the policy)
- claim_type: "auto" for all rows — every incident_type in the source is a
  vehicle collision / theft / parked-car incident
- status: "filed" for all rows — the source has no status field. fraud_reported
  is NOT mapped to a status; it is kept verbatim in `description`
- filed_date = incident_date — the source has no filed date, so we use the
  incident date and say so; do not treat it as a real filing lag
- state: policy_state (OH / IL / IN in this file)

Usage:
    python -m src.ingestion.load_open_data
    python -m src.ingestion.load_open_data --input data/raw_open/insurance_claims.csv --out data/raw_open/insurance_claims_mapped.jsonl
"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


def map_row(row: dict, idx: int) -> dict:
    return {
        "claim_id": f"OPEN-{idx:04d}",
        "policy_id": f"POL-{row['policy_number']}",
        "customer_id": f"CUST-{row['policy_number']}",
        "claim_type": "auto",
        "status": "filed",
        "claim_amount": float(row["total_claim_amount"]),
        "filed_date": row["incident_date"],
        "incident_date": row["incident_date"],
        "state": row["policy_state"],
        "description": (
            f"{row['incident_type']} | {row['incident_severity']} | "
            f"fraud_reported={row['fraud_reported']} | "
            f"{row['auto_make']} {row['auto_model']} {row['auto_year']}"
        ),
    }


def load(input_path: Path, out_path: Path) -> int:
    with input_path.open(newline="") as f:
        rows = list(csv.DictReader(f))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w") as f:
        for i, row in enumerate(rows, start=1):
            f.write(json.dumps(map_row(row, i)) + "\n")
    return len(rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="data/raw_open/insurance_claims.csv")
    ap.add_argument("--out", default="data/raw_open/insurance_claims_mapped.jsonl")
    args = ap.parse_args()
    n = load(Path(args.input), Path(args.out))
    print(f"Mapped {n} open-source claims → {args.out}")


if __name__ == "__main__":
    main()
