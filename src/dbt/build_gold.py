"""Day 5 — dbt Gold marts, stdlib mirror.

Runs the exact transformations the dbt models in ``src/dbt/models``
declare, over the Day 4 enriched Silver JSONL files, and writes Gold
JSONL into ``data/gold``:

- ``fct_claims`` — one row per claim (stg_claims pass-through, deduped
  by claim_id keeping the latest ``ingested_at``).
- ``dim_customer`` / ``dim_policy`` — derived from the claims
  themselves; no external master exists (Day 4 note), so names are
  first-seen and counts come from the claims present.
- ``agg_claims_daily_state`` — one row per (filed_date, state,
  claim_type); totals must reconcile to ``fct_claims``.

dbt itself is not installed in this VM (no dbt-core / dbt-duckdb),
so ``dbt build`` is documented, not run here — see docs/dbt_gold.md.
This mirror is what the tests verify.

Usage:
    python -m src.dbt.build_gold --silver data/silver_enriched --out data/gold
    # or: make gold
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path

GOLD_FILES = ("fct_claims", "dim_customer", "dim_policy", "agg_claims_daily_state")


def load_silver_enriched(silver: Path) -> list[dict]:
    """Read all enriched-Silver part files, skipping the quarantine dir."""
    rows: list[dict] = []
    for part in sorted(silver.glob("filed_date=*/part.jsonl")):
        with part.open() as fh:
            for line in fh:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
    return rows


def build_stg(rows: list[dict]) -> list[dict]:
    """stg_claims: cast + normalise; mirrors models/staging/stg_claims.sql."""
    out = []
    for r in rows:
        out.append({
            "claim_id": str(r["claim_id"]),
            "policy_id": str(r["policy_id"]),
            "customer_id": str(r["customer_id"]),
            "customer_name": str(r.get("customer_name") or ""),
            "claim_type": str(r["claim_type"]).strip().lower(),
            "status": str(r["status"]).strip().lower(),
            "claim_amount": float(r["claim_amount"]),
            "filed_date": str(r["filed_date"]),
            "incident_date": str(r["incident_date"]),
            "state": str(r["state"]).strip().upper(),
            "description": str(r.get("description") or ""),
            "claim_age_days": int(r["claim_age_days"]),
            "is_high_value": bool(r["is_high_value"]),
            "customer_claim_count": int(r.get("customer_claim_count", 0)),
            "policy_claim_count": int(r.get("policy_claim_count", 0)),
            "ingested_at": str(r.get("ingested_at") or ""),
        })
    return out


def build_fct(stg: list[dict]) -> list[dict]:
    """fct_claims: one row per claim; dedupe keeps latest ingested_at."""
    best: dict[str, dict] = {}
    for r in stg:
        cur = best.get(r["claim_id"])
        if cur is None or r["ingested_at"] >= cur["ingested_at"]:
            best[r["claim_id"]] = r
    return [best[k] for k in sorted(best)]


def build_dim_customer(fct: list[dict]) -> list[dict]:
    groups: dict[str, list[dict]] = defaultdict(list)
    for r in fct:
        groups[r["customer_id"]].append(r)
    dims = []
    for cid in sorted(groups):
        rows = sorted(groups[cid], key=lambda x: x["filed_date"])
        name = next((x["customer_name"] for x in rows if x["customer_name"]), "")
        dims.append({
            "customer_id": cid,
            "customer_name": name,
            "first_filed_date": rows[0]["filed_date"],
            "last_filed_date": rows[-1]["filed_date"],
            "claim_count": len(rows),
            "total_claim_amount": round(sum(x["claim_amount"] for x in rows), 2),
            "high_value_claim_count": sum(1 for x in rows if x["is_high_value"]),
        })
    return dims


def build_dim_policy(fct: list[dict]) -> list[dict]:
    groups: dict[str, list[dict]] = defaultdict(list)
    for r in fct:
        groups[r["policy_id"]].append(r)
    dims = []
    for pid in sorted(groups):
        rows = sorted(groups[pid], key=lambda x: x["filed_date"])
        dims.append({
            "policy_id": pid,
            "first_filed_date": rows[0]["filed_date"],
            "last_filed_date": rows[-1]["filed_date"],
            "claim_count": len(rows),
            "total_claim_amount": round(sum(x["claim_amount"] for x in rows), 2),
            "high_value_claim_count": sum(1 for x in rows if x["is_high_value"]),
        })
    return dims


def build_agg(fct: list[dict]) -> list[dict]:
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for r in fct:
        groups[(r["filed_date"], r["state"], r["claim_type"])].append(r)
    aggs = []
    for key in sorted(groups):
        rows = groups[key]
        total = round(sum(x["claim_amount"] for x in rows), 2)
        aggs.append({
            "filed_date": key[0],
            "state": key[1],
            "claim_type": key[2],
            "claim_count": len(rows),
            "customer_count": len({x["customer_id"] for x in rows}),
            "policy_count": len({x["policy_id"] for x in rows}),
            "total_claim_amount": total,
            "avg_claim_amount": round(total / len(rows), 2),
            "high_value_claim_count": sum(1 for x in rows if x["is_high_value"]),
        })
    return aggs


def build_gold(silver: Path, out: Path) -> dict:
    """Build all Gold tables; returns counts + reconciliation totals."""
    rows = load_silver_enriched(silver)
    stg = build_stg(rows)
    fct = build_fct(stg)
    dims_c = build_dim_customer(fct)
    dims_p = build_dim_policy(fct)
    agg = build_agg(fct)

    out.mkdir(parents=True, exist_ok=True)
    tables = {
        "fct_claims": fct,
        "dim_customer": dims_c,
        "dim_policy": dims_p,
        "agg_claims_daily_state": agg,
    }
    for name, table in tables.items():
        with (out / f"{name}.jsonl").open("w") as fh:
            for row in table:
                fh.write(json.dumps(row) + "\n")

    manifest = {
        "source_rows": len(rows),
        "fct_claims": len(fct),
        "dim_customer": len(dims_c),
        "dim_policy": len(dims_p),
        "agg_claims_daily_state": len(agg),
        "fct_total_claim_amount": round(sum(r["claim_amount"] for r in fct), 2),
        "agg_total_claim_amount": round(sum(r["total_claim_amount"] for r in agg), 2),
        "agg_claim_count": sum(r["claim_count"] for r in agg),
        "high_value_claims": sum(1 for r in fct if r["is_high_value"]),
    }
    with (out / "manifest.json").open("w") as fh:
        json.dump(manifest, fh, indent=2)
    return manifest


def main() -> None:
    ap = argparse.ArgumentParser(description="Build dbt Gold marts (stdlib mirror).")
    ap.add_argument("--silver", default="data/silver_enriched")
    ap.add_argument("--out", default="data/gold")
    args = ap.parse_args()
    manifest = build_gold(Path(args.silver), Path(args.out))
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
