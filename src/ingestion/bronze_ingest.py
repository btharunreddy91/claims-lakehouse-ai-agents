"""Day 1 — Bronze ingestion.

Reads raw claims JSONL, validates each row against src.common.schemas.Claim,
writes valid rows to data/bronze/filed_date=YYYY-MM-DD/part.jsonl
and invalid rows to data/bronze/_quarantine/bad_rows.jsonl (never dropped silently).

Usage:
    python -m src.ingestion.bronze_ingest --input data/raw/claims.jsonl
    python -m src.ingestion.bronze_ingest --input data/raw/claims.jsonl --bronze data/bronze
"""
from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path

from src.common.schemas import Claim


def ingest(input_path: Path, bronze_dir: Path) -> dict:
    counts = Counter()
    by_date: dict[str, list[dict]] = defaultdict(list)
    bad: list[dict] = []

    for line in input_path.read_text().splitlines():
        if not line.strip():
            continue
        counts["read"] += 1
        try:
            row = json.loads(line)
            claim = Claim(**{k: row.get(k, "") for k in
                             ["claim_id", "policy_id", "customer_id", "claim_type",
                              "status", "claim_amount", "filed_date", "incident_date",
                              "state", "description"]})
            errors = claim.validate()
        except Exception as e:  # malformed JSON / wrong types
            errors = [f"parse error: {e}"]
            row = {"_raw": line}
        if errors:
            counts["quarantined"] += 1
            bad.append({"row": row, "errors": errors})
        else:
            counts["valid"] += 1
            by_date[claim.filed_date].append(row)

    for filed_date, rows in by_date.items():
        part = bronze_dir / f"filed_date={filed_date}"
        part.mkdir(parents=True, exist_ok=True)
        with (part / "part.jsonl").open("w") as f:
            for r in rows:
                f.write(json.dumps(r) + "\n")

    if bad:
        q = bronze_dir / "_quarantine"
        q.mkdir(parents=True, exist_ok=True)
        with (q / "bad_rows.jsonl").open("w") as f:
            for b in bad:
                f.write(json.dumps(b) + "\n")

    return dict(counts)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--bronze", default="data/bronze")
    args = ap.parse_args()
    result = ingest(Path(args.input), Path(args.bronze))
    print(f"Bronze ingest: {result}")


if __name__ == "__main__":
    main()
