"""Day 6 — Analytics Agent v1: text-to-SQL over the Gold marts.

LangGraph 101 in this repo: a typed state, nodes that each do one job,
tools the nodes call, and a router that decides the next node.

    question
      -> plan     (rule-based planner; see plan_with_llm for the LLM seam)
      -> validate (read-only SQL guardrail — single SELECT over Gold only)
      -> execute  (run the SQL against Gold loaded into in-memory SQLite)
      -> answer   (human answer + the SQL, rows PII-masked)

Real LangGraph when it is installed (``langgraph==1.2.22`` in the repo
``.venv`` — verified Nov 15), otherwise a stdlib mirror that runs the
exact same node functions in the exact same order. The mirror is the
fallback, not a second implementation: state, nodes, tools, planner,
validator and executor are shared. See docs/agents.md.

Guardrails (enforced in code, not just in the prompt):
- Gold only: ``fct_claims``, ``dim_customer``, ``dim_policy``,
  ``agg_claims_daily_state``. No other table can be queried.
- Read-only: the SQL must be a single SELECT. INSERT / UPDATE / DELETE /
  DROP / ALTER / CREATE / TRUNCATE / ... and multi-statement SQL are
  rejected, including statements smuggled inside comments.
- Row limit: default 100, hard max 1000.
- PII: customer names are reduced to initials and policy/customer ids
  to their last 4 characters in every answer. The SQL is always shown.

No LLM is called anywhere in v1. The planner is deterministic and
rule-based; ``plan_with_llm`` is the seam where a real LLM plugs in
later (Day 9+). No API key, no network dependency.

Usage:
    python -m src.agents.analytics "total claim amount by state"
    python -m src.agents.cli "high-value claims"
    python -m src.agents.analytics "claims between 2026-01-01 and 2026-03-31"
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from pathlib import Path
from typing import Any, Callable, Optional

# ---------------------------------------------------------------------------
# LangGraph: real when installed, stdlib mirror otherwise (repo pattern,
# same as the Day 3 Spark mirror and the Day 5 dbt mirror).
# ---------------------------------------------------------------------------
try:
    from langgraph.graph import END, StateGraph
    from typing_extensions import TypedDict as _TypedDict

    LANGGRAPH_AVAILABLE = True
except Exception:  # pragma: no cover - exercised by the mirror tests
    LANGGRAPH_AVAILABLE = False
    from typing import TypedDict as _TypedDict

    END = "__end__"  # type: ignore[assignment]

# ---------------------------------------------------------------------------
# Gold schema (mirrors src/dbt/build_gold.py — do not re-implement Gold)
# ---------------------------------------------------------------------------
GOLD_TABLES: dict[str, tuple[str, ...]] = {
    "fct_claims": (
        "claim_id", "policy_id", "customer_id", "customer_name",
        "claim_type", "status", "claim_amount", "filed_date",
        "incident_date", "state", "description", "claim_age_days",
        "is_high_value", "customer_claim_count", "policy_claim_count",
        "ingested_at",
    ),
    "dim_customer": (
        "customer_id", "customer_name", "first_filed_date",
        "last_filed_date", "claim_count", "total_claim_amount",
        "high_value_claim_count",
    ),
    "dim_policy": (
        "policy_id", "first_filed_date", "last_filed_date",
        "claim_count", "total_claim_amount", "high_value_claim_count",
    ),
    "agg_claims_daily_state": (
        "filed_date", "state", "claim_type", "claim_count",
        "customer_count", "policy_count", "total_claim_amount",
        "avg_claim_amount", "high_value_claim_count",
    ),
}

CLAIM_TYPES = ("auto", "property", "health", "life")
WRITE_KEYWORDS = (
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "TRUNCATE",
    "REPLACE", "MERGE", "GRANT", "REVOKE", "PRAGMA", "ATTACH", "DETACH",
    "VACUUM", "COPY", "INTO", "CALL", "EXEC", "EXECUTE",
)
ALLOWED_FUNCTIONS = ("COUNT", "SUM", "AVG", "MIN", "MAX", "ROUND", "COALESCE")
DEFAULT_LIMIT = 100
MAX_LIMIT = 1000

SUPPORTED_QUESTIONS = (
    "total claims / total claim amount (overall)",
    "total claim amount by state",
    "total claim amount by claim type",
    "total claim amount by status",
    "top N states by claim amount",
    "top N customers by total claim amount",
    "high-value claims (over $25,000)",
    "claims in a state (e.g. claims in TX)",
    "claims between two dates (YYYY-MM-DD and YYYY-MM-DD)",
    "average claim amount",
    "claims per day",
)


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------
class AgentState(_TypedDict, total=False):
    """Typed agent state — the single source of truth passed node to node."""

    question: str
    intent: Optional[str]
    sql: Optional[str]
    rows: list[dict]
    answer: Optional[str]
    errors: list[str]
    row_limit: int
    engine: str  # "langgraph" or "mirror"


def new_state(question: str, row_limit: int = DEFAULT_LIMIT) -> AgentState:
    return {
        "question": question,
        "intent": None,
        "sql": None,
        "rows": [],
        "answer": None,
        "errors": [],
        "row_limit": max(1, min(int(row_limit), MAX_LIMIT)),
        "engine": "langgraph" if LANGGRAPH_AVAILABLE else "mirror",
    }


# ---------------------------------------------------------------------------
# Tool 1 — read-only SQL validator
# ---------------------------------------------------------------------------
def validate_sql(sql: str) -> list[str]:
    """Return a list of guardrail violations; empty means the SQL is OK.

    Rejects: anything that is not a single SELECT; multiple statements;
    comments smuggling a second statement or a write keyword; write
    keywords anywhere; UNION/EXCEPT/INTERSECT (v1: one query only);
    subqueries; tables outside the Gold allowlist; functions outside
    the small aggregate allowlist.
    """
    errors: list[str] = []
    if not sql or not sql.strip():
        return ["empty SQL"]

    # 1. Comments: inspect them, then strip them — a comment must not
    #    smuggle a second statement or a write keyword.
    comments = re.findall(r"/\*.*?\*/", sql, flags=re.S)
    comments += re.findall(r"--[^\n]*", sql)
    for c in comments:
        if ";" in c:
            errors.append("comment contains a statement separator (;)")
        for kw in WRITE_KEYWORDS:
            if re.search(rf"\b{kw}\b", c, flags=re.I):
                errors.append(f"comment contains forbidden keyword {kw}")
    decommented = re.sub(r"/\*.*?\*/", " ", sql, flags=re.S)
    decommented = re.sub(r"--[^\n]*", " ", decommented)

    # 2. Single statement: at most one semicolon, and only at the end.
    stripped = decommented.strip()
    if stripped.endswith(";"):
        stripped = stripped[:-1].rstrip()
    if ";" in stripped:
        errors.append("multiple statements are not allowed")

    upper = stripped.upper()
    if not re.match(r"\s*SELECT\b", stripped, flags=re.I):
        errors.append("only a single SELECT statement is allowed")

    for kw in WRITE_KEYWORDS:
        if re.search(rf"\b{kw}\b", upper):
            errors.append(f"forbidden keyword {kw}")

    for kw in ("UNION", "EXCEPT", "INTERSECT"):
        if re.search(rf"\b{kw}\b", upper):
            errors.append(f"{kw} is not supported in v1 (single query only)")

    if re.search(r"\(\s*SELECT\b", upper):
        errors.append("subqueries are not supported in v1")

    # 3. Tables: only Gold tables, after FROM / JOIN.
    tables = re.findall(r"\b(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_]*)", stripped)
    if not tables:
        errors.append("no FROM clause found")
    for t in tables:
        if t.lower() not in GOLD_TABLES:
            errors.append(f"table {t!r} is not a Gold mart (allowed: {sorted(GOLD_TABLES)})")

    # 4. Functions: only the small aggregate allowlist.
    for fn in re.findall(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(", stripped):
        if fn.upper() not in ALLOWED_FUNCTIONS:
            errors.append(f"function {fn}() is not allowed in v1")

    return errors


def apply_limit(sql: str, limit: int) -> str:
    """Clamp the query to at most ``limit`` rows (limit is 1..1000)."""
    limit = max(1, min(int(limit), MAX_LIMIT))
    s = sql.strip()
    trailing = s.endswith(";")
    if trailing:
        s = s[:-1].rstrip()
    m = re.search(r"\bLIMIT\s+\d+\s*$", s, flags=re.I)
    if m:
        existing = int(re.search(r"\d+", m.group(0)).group(0))
        s = s[: m.start()] + f"LIMIT {min(existing, limit)}"
    else:
        s = f"{s} LIMIT {limit}"
    return s + (";" if trailing else "")


# ---------------------------------------------------------------------------
# Tool 2 — PII masking (applied to every answer row, never optional)
# ---------------------------------------------------------------------------
def mask_value(key: str, value: Any) -> Any:
    if value is None:
        return None
    if key == "customer_name":
        parts = str(value).split()
        if len(parts) >= 2:
            return f"{parts[0][0]}. {parts[-1][0]}."
        return f"{str(value)[0]}." if value else ""
    if key in ("policy_id", "customer_id"):
        s = str(value)
        return f"****{s[-4:]}" if len(s) > 4 else "****"
    return value


def mask_row(row: dict) -> dict:
    return {k: mask_value(k, v) for k, v in row.items()}


# ---------------------------------------------------------------------------
# Tool 3 — Gold loader (reuses Day 5 build_gold; executes via SQLite)
# ---------------------------------------------------------------------------
def load_gold(gold_dir: Path) -> dict[str, list[dict]]:
    """Read the Gold JSONL tables (build them first if missing)."""
    gold_dir = Path(gold_dir)
    if not (gold_dir / "fct_claims.jsonl").exists():
        from src.dbt.build_gold import build_gold

        repo = Path(__file__).resolve().parents[2]
        build_gold(repo / "data" / "silver_enriched", gold_dir)
    tables: dict[str, list[dict]] = {}
    for name in GOLD_TABLES:
        path = gold_dir / f"{name}.jsonl"
        tables[name] = [json.loads(l) for l in path.read_text().splitlines() if l.strip()]
    return tables


def _sqlite_type(value: Any) -> str:
    if isinstance(value, bool):
        return "INTEGER"
    if isinstance(value, int):
        return "INTEGER"
    if isinstance(value, float):
        return "REAL"
    return "TEXT"


def execute_sql(sql: str, gold_dir: Path, limit: int = DEFAULT_LIMIT) -> list[dict]:
    """Validate, then run a read-only SELECT over Gold in SQLite.

    Raises ValueError with the validator's errors on any rejection.
    """
    errors = validate_sql(sql)
    if errors:
        raise ValueError("; ".join(errors))
    sql = apply_limit(sql, limit)
    tables = load_gold(gold_dir)
    conn = sqlite3.connect(":memory:")
    try:
        for name, rows in tables.items():
            cols = GOLD_TABLES[name]
            sample = rows[0] if rows else {}
            defs = ", ".join(
                f'"{c}" {_sqlite_type(sample.get(c, ""))}' for c in cols
            )
            conn.execute(f'CREATE TABLE "{name}" ({defs})')
            placeholders = ", ".join("?" for _ in cols)
            conn.executemany(
                f'INSERT INTO "{name}" VALUES ({placeholders})',
                [
                    tuple(
                        (1 if r.get(c) is True else 0 if r.get(c) is False else r.get(c))
                        for c in cols
                    )
                    for r in rows
                ],
            )
        cur = conn.execute(sql)
        columns = [d[0] for d in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Planner — deterministic, rule-based, offline (v1). No LLM, no API key.
# ---------------------------------------------------------------------------
def _state_code(question_upper: str) -> Optional[str]:
    """Extract a 2-letter state code after 'in'/'for' (validated)."""
    try:
        from src.common.schemas import US_STATES
    except Exception:
        US_STATES = ()
    m = re.search(r"\b(?:IN|FOR)\s+([A-Z]{2})\b", question_upper)
    if m and (not US_STATES or m.group(1) in US_STATES):
        return m.group(1)
    return None


def plan_question(question: str) -> tuple[Optional[str], Optional[str]]:
    """Rule-based text-to-SQL. Returns (intent, sql); (None, None) if the
    question is not one of the supported intents — never guess SQL."""
    q = (question or "").strip()
    if not q:
        return None, None
    u = q.upper()
    low = q.lower()

    # Date range first: it carries its own literals.
    m = re.search(r"(\d{4}-\d{2}-\d{2})\s*(?:TO|AND|-|–)\s*(\d{4}-\d{2}-\d{2})", u)
    if m and ("BETWEEN" in u or "FROM" in u or "CLAIM" in u):
        start, end = m.group(1), m.group(2)
        if start <= end:
            return (
                "claims_in_date_range",
                "SELECT claim_id, state, claim_type, claim_amount, filed_date "
                "FROM fct_claims "
                f"WHERE filed_date BETWEEN '{start}' AND '{end}' "
                "ORDER BY filed_date, claim_amount DESC",
            )
        return None, None

    if (
        ("HIGH" in u and "VALUE" in u)
        or re.search(r"OVER\s*\$?\s*25", u) is not None
        or ">= 25000" in q.replace(",", "")
        or "AT LEAST $25" in u
    ):
        return (
            "high_value_claims",
            "SELECT claim_id, state, claim_type, claim_amount, filed_date "
            "FROM fct_claims WHERE is_high_value = 1 "
            "ORDER BY claim_amount DESC",
        )

    if "TOP" in u and "STATE" in u:
        n = re.search(r"TOP\s+(\d+)", u)
        limit = int(n.group(1)) if n else 5
        limit = max(1, min(limit, 50))
        return (
            "top_states_by_amount",
            "SELECT state, COUNT(*) AS claim_count, "
            "SUM(claim_amount) AS total_claim_amount "
            "FROM fct_claims GROUP BY state "
            "ORDER BY total_claim_amount DESC "
            f"LIMIT {limit}",
        )

    if "TOP" in u and "CUSTOMER" in u:
        n = re.search(r"TOP\s+(\d+)", u)
        limit = int(n.group(1)) if n else 5
        limit = max(1, min(limit, 50))
        return (
            "top_customers_by_amount",
            "SELECT customer_id, customer_name, claim_count, "
            "total_claim_amount FROM dim_customer "
            "ORDER BY total_claim_amount DESC "
            f"LIMIT {limit}",
        )

    if "BY STATE" in u or "PER STATE" in u:
        return (
            "total_amount_by_state",
            "SELECT state, COUNT(*) AS claim_count, "
            "SUM(claim_amount) AS total_claim_amount "
            "FROM fct_claims GROUP BY state "
            "ORDER BY total_claim_amount DESC",
        )

    if "BY CLAIM TYPE" in u or "BY TYPE" in u or "PER CLAIM TYPE" in u:
        return (
            "total_amount_by_claim_type",
            "SELECT claim_type, COUNT(*) AS claim_count, "
            "SUM(claim_amount) AS total_claim_amount "
            "FROM fct_claims GROUP BY claim_type "
            "ORDER BY total_claim_amount DESC",
        )

    if "BY STATUS" in u or "PER STATUS" in u:
        return (
            "total_amount_by_status",
            "SELECT status, COUNT(*) AS claim_count, "
            "SUM(claim_amount) AS total_claim_amount "
            "FROM fct_claims GROUP BY status "
            "ORDER BY total_claim_amount DESC",
        )

    if "PER DAY" in u or "DAILY" in u or "BY DAY" in u or "BY DATE" in u:
        return (
            "claims_per_day",
            "SELECT filed_date, SUM(claim_count) AS claim_count, "
            "SUM(total_claim_amount) AS total_claim_amount "
            "FROM agg_claims_daily_state GROUP BY filed_date "
            "ORDER BY filed_date",
        )

    if "AVERAGE" in u or "AVG " in u or "MEAN" in u:
        return (
            "average_claim_amount",
            "SELECT COUNT(*) AS claim_count, AVG(claim_amount) AS avg_claim_amount, "
            "SUM(claim_amount) AS total_claim_amount FROM fct_claims",
        )

    state = _state_code(u)
    if state and ("CLAIM" in u or "TOTAL" in u or "AMOUNT" in u):
        return (
            "claims_in_state",
            "SELECT claim_id, claim_type, status, claim_amount, filed_date "
            f"FROM fct_claims WHERE state = '{state}' "
            "ORDER BY claim_amount DESC",
        )

    if ("TOTAL" in u or "HOW MANY" in u) and "CLAIM" in u:
        return (
            "total_claims",
            "SELECT COUNT(*) AS claim_count, SUM(claim_amount) AS total_claim_amount "
            "FROM fct_claims",
        )

    return None, None


def plan_with_llm(
    question: str,
    llm: Optional[Callable[[str], str]] = None,
    schema: Optional[dict] = None,
) -> tuple[str, str]:
    """Seam where a real LLM replaces the rule planner (Day 9+).

    Pass an ``llm`` callable that takes a prompt and returns a SQL string;
    the returned SQL is still run through :func:`validate_sql` before it
    can execute. With no LLM configured this raises NotImplementedError —
    v1 never invents an API key or a network call.
    """
    if llm is None:
        raise NotImplementedError(
            "No LLM configured for the Analytics Agent yet — v1 uses the "
            "deterministic rule planner (plan_question). Pass an llm "
            "callable to plan_with_llm to enable LLM planning."
        )
    schema = schema or {t: list(c) for t, c in GOLD_TABLES.items()}
    prompt = (
        "Write one read-only SQLite SELECT over these Gold tables "
        f"(schema: {json.dumps(schema)}). Question: {question}"
    )
    sql = llm(prompt)
    errors = validate_sql(sql)
    if errors:
        raise ValueError("LLM SQL rejected by validator: " + "; ".join(errors))
    return "llm_planned", sql


# ---------------------------------------------------------------------------
# Nodes — one job each; shared by the real graph and the mirror.
# ---------------------------------------------------------------------------
def plan_node(state: AgentState) -> dict:
    intent, sql = plan_question(state["question"])
    if sql is None:
        return {
            "intent": None,
            "sql": None,
            "errors": list(state.get("errors", []))
            + [
                "Question not supported yet — I will not guess SQL. "
                "Supported: " + "; ".join(SUPPORTED_QUESTIONS)
            ],
        }
    return {"intent": intent, "sql": sql}


def validate_node(state: AgentState) -> dict:
    errors = validate_sql(state["sql"] or "")
    if errors:
        return {"errors": list(state.get("errors", [])) + errors, "sql": state["sql"]}
    return {}


def execute_node(state: AgentState, gold_dir: Path | None = None) -> dict:
    gold = gold_dir or Path(__file__).resolve().parents[2] / "data" / "gold"
    try:
        rows = execute_sql(state["sql"] or "", gold, state.get("row_limit", DEFAULT_LIMIT))
    except ValueError as exc:
        return {"errors": list(state.get("errors", [])) + [str(exc)], "rows": []}
    return {"rows": [mask_row(r) for r in rows]}


def answer_node(state: AgentState) -> dict:
    if state.get("errors"):
        return {
            "answer": "I could not answer that safely: " + "; ".join(state["errors"])
        }
    rows = state.get("rows", [])
    intent = state.get("intent")
    # List answers only ever describe the rows actually returned: when
    # the row limit caps the result, say so — never present a capped
    # slice as the whole population.
    capped = (
        f" (row limit of {state.get('row_limit', DEFAULT_LIMIT)} reached — "
        "showing the first rows only)"
        if rows and len(rows) >= state.get("row_limit", DEFAULT_LIMIT)
        else ""
    )
    if intent == "total_claims" and rows:
        r = rows[0]
        return {
            "answer": (
                f"{int(r['claim_count']):,} claims totalling "
                f"${float(r['total_claim_amount']):,.2f}."
            )
        }
    if intent in ("total_amount_by_state", "total_amount_by_claim_type",
                  "total_amount_by_status", "top_states_by_amount") and rows:
        total = sum(float(r.get("total_claim_amount") or 0) for r in rows)
        top = rows[0]
        key = "state" if "state" in top else "claim_type" if "claim_type" in top else "status"
        return {
            "answer": (
                f"{len(rows)} groups, ${total:,.2f} in total. "
                f"Top: {top[key]} with ${float(top['total_claim_amount']):,.2f} "
                f"across {int(top['claim_count']):,} claims."
            )
        }
    if intent == "high_value_claims":
        total = sum(float(r.get("claim_amount") or 0) for r in rows)
        return {
            "answer": (
                f"{len(rows):,} high-value claims (>= $25,000) shown, "
                f"totalling ${total:,.2f} over the rows shown.{capped}"
            )
        }
    if intent == "average_claim_amount" and rows:
        r = rows[0]
        return {
            "answer": (
                f"Average claim amount is ${float(r['avg_claim_amount']):,.2f} "
                f"across {int(r['claim_count']):,} claims "
                f"(total ${float(r['total_claim_amount']):,.2f})."
            )
        }
    if intent == "claims_in_date_range":
        total = sum(float(r.get("claim_amount") or 0) for r in rows)
        return {
            "answer": (
                f"Showing {len(rows):,} claims in that date range, "
                f"totalling ${total:,.2f} over the rows shown.{capped}"
            )
        }
    if intent == "claims_in_state":
        total = sum(float(r.get("claim_amount") or 0) for r in rows)
        return {
            "answer": (
                f"Showing {len(rows):,} claims in that state, "
                f"totalling ${total:,.2f} over the rows shown.{capped}"
            )
        }
    if intent == "top_customers_by_amount":
        return {"answer": f"Top {len(rows)} customers by total claim amount (names masked to initials)."}
    if intent == "claims_per_day":
        total = sum(float(r.get("total_claim_amount") or 0) for r in rows)
        return {
            "answer": (
                f"{len(rows)} days shown, ${total:,.2f} in total over the rows shown.{capped}"
            )
        }
    return {"answer": f"{len(rows)} row(s) returned."}


# ---------------------------------------------------------------------------
# Graph — real LangGraph when installed, stdlib mirror otherwise.
# ---------------------------------------------------------------------------
def _route_after_plan(state: AgentState) -> str:
    return "answer" if state.get("errors") or not state.get("sql") else "validate"


def _route_after_validate(state: AgentState) -> str:
    return "answer" if state.get("errors") else "execute"


class _MirrorGraph:
    """Stdlib mirror of the LangGraph graph: same nodes, same routing."""

    def __init__(self, gold_dir: Path | None = None):
        self.gold_dir = gold_dir

    def invoke(self, state: AgentState) -> AgentState:
        s: AgentState = dict(state)  # type: ignore[arg-type]
        s["engine"] = "mirror"
        s.update(plan_node(s))
        if _route_after_plan(s) == "answer":
            s.update(answer_node(s))
            return s
        s.update(validate_node(s))
        if _route_after_validate(s) == "answer":
            s.update(answer_node(s))
            return s
        s.update(execute_node(s, self.gold_dir))
        if s.get("errors"):
            s.update(answer_node(s))
            return s
        s.update(answer_node(s))
        return s


def build_graph(gold_dir: Path | None = None):
    """Return the compiled graph (real LangGraph) or the stdlib mirror."""
    if LANGGRAPH_AVAILABLE:
        graph = StateGraph(AgentState)
        graph.add_node("plan", plan_node)
        graph.add_node("validate", validate_node)
        graph.add_node(
            "execute", lambda s: execute_node(s, gold_dir)
        )
        graph.add_node("answer", answer_node)
        graph.set_entry_point("plan")
        graph.add_conditional_edges("plan", _route_after_plan, {"validate": "validate", "answer": "answer"})
        graph.add_conditional_edges("validate", _route_after_validate, {"execute": "execute", "answer": "answer"})
        graph.add_edge("execute", "answer")
        graph.add_edge("answer", END)
        return graph.compile()
    return _MirrorGraph(gold_dir)


def ask(
    question: str,
    gold_dir: Path | None = None,
    row_limit: int = DEFAULT_LIMIT,
    llm: Optional[Callable[[str], str]] = None,
) -> AgentState:
    """Run one question end to end; returns the final agent state.

    ``llm`` is the Day 9+ seam: pass a callable and the plan node is
    bypassed in favour of ``plan_with_llm``. v1 callers pass nothing.
    """
    state = new_state(question, row_limit)
    if llm is not None:
        try:
            intent, sql = plan_with_llm(question, llm=llm)
            state.update({"intent": intent, "sql": sql})
        except (NotImplementedError, ValueError) as exc:
            state["errors"].append(str(exc))
            state.update(answer_node(state))
            return state
        state.update(validate_node(state))
        if not state["errors"]:
            state.update(execute_node(state, gold_dir))
        state.update(answer_node(state))
        return state
    graph = build_graph(gold_dir)
    return graph.invoke(state)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Analytics Agent v1 — ask a question over the Gold marts."
    )
    ap.add_argument("question", help="e.g. 'total claim amount by state'")
    ap.add_argument("--gold", default=None, help="Gold dir (default: data/gold)")
    ap.add_argument("--limit", type=int, default=DEFAULT_LIMIT,
                    help=f"row limit, default {DEFAULT_LIMIT}, max {MAX_LIMIT}")
    ap.add_argument("--rebuild-gold", action="store_true",
                    help="rebuild Gold from enriched Silver first")
    args = ap.parse_args(argv)

    repo = Path(__file__).resolve().parents[2]
    gold_dir = Path(args.gold) if args.gold else repo / "data" / "gold"
    if args.rebuild_gold:
        from src.dbt.build_gold import build_gold

        print(json.dumps(build_gold(repo / "data" / "silver_enriched", gold_dir), indent=2))

    result = ask(args.question, gold_dir=gold_dir, row_limit=args.limit)
    print(f"Question: {result['question']}")
    print(f"Engine:   {result['engine']} (intent: {result.get('intent')})")
    print(f"SQL:\n  {result.get('sql') or '(no SQL — question not supported yet)'}")
    if result.get("errors"):
        print("Errors:")
        for e in result["errors"]:
            print(f"  - {e}")
    print(f"Rows:     {len(result.get('rows', []))} (limit {result['row_limit']}, masked)")
    if result.get("answer"):
        print(f"Answer:   {result['answer']}")
    for row in result.get("rows", [])[:10]:
        print("  " + json.dumps(row))
    if len(result.get("rows", [])) > 10:
        print(f"  ... and {len(result['rows']) - 10} more row(s)")
    return 0 if not result.get("errors") else 1


if __name__ == "__main__":
    sys.exit(main())
