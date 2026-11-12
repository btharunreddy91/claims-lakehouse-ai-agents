"""Day 3 — Silver transform: Bronze -> Silver.

This is the Spark-free mirror of ``src/streaming/bronze_to_silver.py``.
It applies exactly the same rules the Structured Streaming job applies,
so the logic can be tested and run on this VM (no Java / Spark here):

1. Type casting — ``claim_amount`` to float, ``filed_date`` /
   ``incident_date`` to ISO dates.
2. Validation — rows failing ``Claim.validate()`` go to quarantine,
   never silently dropped.
3. Dedupe — one row per ``claim_id``; the latest ``ingested_at`` wins
   (Kafka is at-least-once, so duplicates are expected).
4. Late-data watermark — the watermark advances as rows *arrive*:
   a row whose ``filed_date`` is more than ``watermark_days`` older
   than the newest ``filed_date`` seen *so far* is dropped as late
   (recorded separately, not quarantined — it was valid, it just
   arrived too late for the streaming state). This mirrors Structured
   Streaming, where the watermark is driven by event times seen so
   far, not by the newest event time in the whole dataset — so a
   backfill of old claims arriving in date order is not wiped out.
   The CLI reads Bronze partitions in ``filed_date`` order, so a claim
   is late only if it lands in a partition much newer than its own
   ``filed_date``.

Usage (dry-run / batch mirror over the Bronze files):
    python -m src.streaming.silver_transform --bronze data/bronze --silver data/silver
"""
from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import date, timedelta
from pathlib import Path

from src.common.schemas import Claim

SILVER_FIELDS = [
    "claim_id", "policy_id", "customer_id", "claim_type", "status",
    "claim_amount", "filed_date", "incident_date", "state",
    "description", "ingested_at",
]


def _to_silver_row(row: dict) -> dict:
    """Cast one validated row to its Silver shape."""
    out = {k: row.get(k, "") for k in SILVER_FIELDS}
    # Keep extra Bronze fields (e.g. customer_name) through Silver;
    # PII masking happens in Gold (Day 5).
    for k, v in row.items():
        if k not in out:
            out[k] = v
    out["claim_amount"] = float(row["claim_amount"])
    out["filed_date"] = date.fromisoformat(str(row["filed_date"])).isoformat()
    out["incident_date"] = date.fromisoformat(str(row["incident_date"])).isoformat()
    return out


def transform_rows(rows: list[dict], watermark_days: int = 7) -> dict:
    """Apply the Silver rules to in-memory Bronze rows.

    Returns {"silver": [...], "quarantined": [...], "late": [...],
    "counts": {...}}.
    """
    counts = Counter()
    valid: list[dict] = []
    quarantined: list[dict] = []

    for row in rows:
        counts["read"] += 1
        try:
            claim = Claim(**{k: row.get(k, "") for k in
                             ["claim_id", "policy_id", "customer_id", "claim_type",
                              "status", "claim_amount", "filed_date", "incident_date",
                              "state", "description"]})
            errors = claim.validate()
        except Exception as e:  # malformed row / wrong types
            errors = [f"parse error: {e}"]
        if errors:
            counts["quarantined"] += 1
            quarantined.append({"row": row, "errors": errors})
        else:
            valid.append(row)

    # Dedupe by claim_id — latest ingested_at wins; ties break on filed_date.
    deduped: dict[str, dict] = {}
    for row in valid:
        key = row["claim_id"]
        prev = deduped.get(key)
        if prev is None or (str(row.get("ingested_at", "")), str(row.get("filed_date", ""))) >= \
                (str(prev.get("ingested_at", "")), str(prev.get("filed_date", ""))):
            deduped[key] = row
    counts["duplicates_dropped"] = len(valid) - len(deduped)

    # Late-data watermark on filed_date, advancing as rows arrive
    # (streaming semantics: driven by event times seen so far).
    silver: list[dict] = []
    late: list[dict] = []
    max_filed: date | None = None
    for row in deduped.values():
        filed = date.fromisoformat(str(row["filed_date"]))
        if max_filed is not None and filed < max_filed - timedelta(days=watermark_days):
            late.append(row)
        else:
            silver.append(_to_silver_row(row))
            if max_filed is None or filed > max_filed:
                max_filed = filed
    counts["late_dropped"] = len(late)
    counts["silver"] = len(silver)
    return {"silver": silver, "quarantined": quarantined, "late": late,
            "counts": dict(counts)}


def load_bronze(bronze_dir: Path) -> list[dict]:
    """Read all Bronze part.jsonl files (skips _quarantine)."""
    rows: list[dict] = []
    for part in sorted(bronze_dir.glob("filed_date=*/part.jsonl")):
        for line in part.read_text().splitlines():
            if line.strip():
                rows.append(json.loads(line))
    return rows


def write_silver(silver_rows: list[dict], silver_dir: Path) -> dict:
    """Write Silver rows partitioned by filed_date (JSONL, Delta in the Spark job)."""
    by_date: dict[str, list[dict]] = {}
    for row in silver_rows:
        by_date.setdefault(row["filed_date"], []).append(row)
    for filed_date, rows in by_date.items():
        part = silver_dir / f"filed_date={filed_date}"
        part.mkdir(parents=True, exist_ok=True)
        with (part / "part.jsonl").open("w") as f:
            for r in rows:
                f.write(json.dumps(r) + "\n")
    return {"partitions": len(by_date), "rows": len(silver_rows)}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bronze", default="data/bronze")
    ap.add_argument("--silver", default="data/silver")
    ap.add_argument("--watermark-days", type=int, default=7)
    args = ap.parse_args()

    bronze_dir, silver_dir = Path(args.bronze), Path(args.silver)
    rows = load_bronze(bronze_dir)
    result = transform_rows(rows, watermark_days=args.watermark_days)
    written = write_silver(result["silver"], silver_dir)
    if result["quarantined"]:
        q = silver_dir / "_quarantine"
        q.mkdir(parents=True, exist_ok=True)
        with (q / "bad_rows.jsonl").open("w") as f:
            for b in result["quarantined"]:
                f.write(json.dumps(b) + "\n")
    if result["late"]:
        q = silver_dir / "_late"
        q.mkdir(parents=True, exist_ok=True)
        with (q / "late_rows.jsonl").open("w") as f:
            for r in result["late"]:
                f.write(json.dumps(r) + "\n")
    print(f"Silver transform: {result['counts']} -> {written}")


if __name__ == "__main__":
    main()
