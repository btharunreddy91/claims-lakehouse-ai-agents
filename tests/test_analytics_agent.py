"""Day 6 — Analytics Agent v1 tests.

Covers the read-only validator, the rule planner, execution over the
real Gold marts (reconciliation), PII masking, the unknown-question
path, row limits, and both graph engines (real LangGraph when
installed, stdlib mirror otherwise).
"""
import pytest

from src.agents.analytics import (
    GOLD_TABLES,
    MAX_LIMIT,
    _MirrorGraph,
    ask,
    execute_sql,
    mask_row,
    new_state,
    plan_question,
    plan_with_llm,
    validate_sql,
)
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
GOLD = REPO / "data" / "gold"

# The Day 5 verified totals — these are real numbers from the Gold
# manifest, not fixtures: 758 claims, $11,975,925.09, 166 high-value.
TOTAL_AMOUNT = 11975925.09
TOTAL_CLAIMS = 758
HIGH_VALUE = 166


# ---------------------------------------------------------------------------
# Validator
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("bad", [
    "INSERT INTO fct_claims VALUES (1, 2, 3)",
    "UPDATE fct_claims SET claim_amount = 0",
    "DELETE FROM fct_claims WHERE claim_id = 'CLM-1'",
    "DROP TABLE fct_claims",
    "ALTER TABLE fct_claims ADD COLUMN x TEXT",
    "CREATE TABLE evil (x TEXT)",
    "TRUNCATE TABLE fct_claims",
    "SELECT * FROM fct_claims; DROP TABLE fct_claims",
    "SELECT * FROM fct_claims; SELECT * FROM dim_customer",
    "SELECT * FROM fct_claims -- ; DROP TABLE fct_claims",
    "SELECT * FROM fct_claims /* ; DELETE FROM fct_claims */",
    "SELECT * FROM fct_claims /* DROP TABLE fct_claims */",
    "SELECT * FROM bronze_claims",
    "SELECT * FROM fct_claims UNION SELECT * FROM dim_customer",
    "WITH x AS (SELECT * FROM fct_claims) SELECT * FROM x",
    "SELECT * FROM (SELECT * FROM fct_claims)",
    "DELETE FROM fct_claims",
    "select claim_id from fct_claims; delete from fct_claims where 1=1",
])
def test_validator_rejects_writes_and_multi_statement(bad):
    assert validate_sql(bad), f"expected rejection: {bad!r}"


@pytest.mark.parametrize("good", [
    "SELECT COUNT(*), SUM(claim_amount) FROM fct_claims",
    "SELECT state, COUNT(*) FROM fct_claims GROUP BY state ORDER BY 2 DESC",
    "SELECT * FROM dim_customer ORDER BY total_claim_amount DESC LIMIT 5",
    "SELECT filed_date, SUM(total_claim_amount) FROM agg_claims_daily_state GROUP BY filed_date",
])
def test_validator_accepts_planner_style_selects(good):
    assert validate_sql(good) == []


def test_validator_allows_single_trailing_semicolon():
    assert validate_sql("SELECT * FROM fct_claims;") == []


# ---------------------------------------------------------------------------
# Planner
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("question,intent", [
    ("total claim amount", "total_claims"),
    ("how many claims are there", "total_claims"),
    ("total claim amount by state", "total_amount_by_state"),
    ("total claim amount by claim type", "total_amount_by_claim_type"),
    ("total claim amount by status", "total_amount_by_status"),
    ("top 5 states by claim amount", "top_states_by_amount"),
    ("top 10 customers by total claim amount", "top_customers_by_amount"),
    ("high-value claims", "high_value_claims"),
    ("claims over $25,000", "high_value_claims"),
    ("claims in TX", "claims_in_state"),
    ("claims between 2026-01-01 and 2026-03-31", "claims_in_date_range"),
    ("average claim amount", "average_claim_amount"),
    ("claims per day", "claims_per_day"),
])
def test_planner_produces_select_for_each_intent(question, intent):
    got_intent, sql = plan_question(question)
    assert got_intent == intent
    assert sql is not None and sql.strip().upper().startswith("SELECT")
    assert validate_sql(sql) == []


def test_planner_never_guesses_sql_for_unknown_question():
    intent, sql = plan_question("will it rain in Dallas tomorrow")
    assert intent is None and sql is None
    intent, sql = plan_question("")
    assert intent is None and sql is None


def test_plan_with_llm_seam_needs_an_llm():
    with pytest.raises(NotImplementedError, match="No LLM configured"):
        plan_with_llm("total claim amount")


def test_plan_with_llm_validates_stub_llm_sql():
    intent, sql = plan_with_llm("total claim amount", llm=lambda p: "SELECT COUNT(*) FROM fct_claims")
    assert intent == "llm_planned"
    assert validate_sql(sql) == []
    with pytest.raises(ValueError, match="rejected by validator"):
        plan_with_llm("total claim amount", llm=lambda p: "DELETE FROM fct_claims")


# ---------------------------------------------------------------------------
# Execution over the real Gold marts
# ---------------------------------------------------------------------------
def test_execution_reconciles_to_gold_total():
    rows = execute_sql(
        "SELECT COUNT(*) AS claim_count, SUM(claim_amount) AS total_claim_amount FROM fct_claims",
        GOLD,
    )
    assert len(rows) == 1
    assert rows[0]["claim_count"] == TOTAL_CLAIMS
    assert rows[0]["total_claim_amount"] == pytest.approx(TOTAL_AMOUNT)


def test_by_state_sums_reconcile_to_total():
    rows = execute_sql(
        "SELECT state, SUM(claim_amount) AS total_claim_amount FROM fct_claims GROUP BY state",
        GOLD,
    )
    assert sum(r["total_claim_amount"] for r in rows) == pytest.approx(TOTAL_AMOUNT)
    assert sum(1 for _ in rows) > 1  # more than one state present


def test_high_value_count_matches_day4():
    rows = execute_sql(
        "SELECT COUNT(*) AS n FROM fct_claims WHERE is_high_value = 1", GOLD
    )
    assert rows[0]["n"] == HIGH_VALUE


def test_execute_sql_refuses_write_even_if_called_directly():
    with pytest.raises(ValueError):
        execute_sql("DELETE FROM fct_claims", GOLD)


# ---------------------------------------------------------------------------
# Masking
# ---------------------------------------------------------------------------
def test_masking_hides_names_and_ids():
    row = {
        "claim_id": "CLM-0000001",
        "policy_id": "POL-80239",
        "customer_id": "CUST-2542",
        "customer_name": "Lena Patel",
        "claim_amount": 13579.11,
    }
    masked = mask_row(row)
    assert masked["customer_name"] == "L. P."
    assert masked["policy_id"] == "****0239"
    assert masked["customer_id"] == "****2542"
    assert "Lena" not in str(masked) and "Patel" not in str(masked)
    assert "POL-80239" not in str(masked) and "CUST-2542" not in str(masked)
    # Non-PII stays usable: claim id and amount are kept.
    assert masked["claim_id"] == "CLM-0000001"
    assert masked["claim_amount"] == 13579.11


def test_end_to_end_rows_are_masked():
    result = ask("top 3 customers by total claim amount", gold_dir=GOLD)
    assert not result["errors"]
    for row in result["rows"]:
        assert "customer_name" not in row or "." in str(row["customer_name"])
        assert len(str(row["customer_name"]).split()) <= 2
        assert str(row["customer_id"]).startswith("****")


# ---------------------------------------------------------------------------
# End to end: ask()
# ---------------------------------------------------------------------------
def test_ask_total_reconciles_and_shows_sql():
    result = ask("total claim amount", gold_dir=GOLD)
    assert not result["errors"]
    assert result["sql"] and "SELECT" in result["sql"]
    assert result["rows"][0]["claim_count"] == TOTAL_CLAIMS
    assert result["rows"][0]["total_claim_amount"] == pytest.approx(TOTAL_AMOUNT)
    assert "758" in result["answer"] and "11,975,925.09" in result["answer"]


def test_ask_by_state_returns_real_groups():
    result = ask("total claim amount by state", gold_dir=GOLD)
    assert not result["errors"]
    assert len(result["rows"]) > 1
    assert sum(r["total_claim_amount"] for r in result["rows"]) == pytest.approx(TOTAL_AMOUNT)


def test_ask_unknown_question_says_not_supported_and_guesses_nothing():
    result = ask("will it rain in Dallas tomorrow", gold_dir=GOLD)
    assert result["sql"] is None
    assert result["rows"] == []
    assert "not supported yet" in result["answer"].lower()
    assert result["errors"]


def test_row_limit_is_clamped_to_max():
    state = new_state("high-value claims", row_limit=5000)
    assert state["row_limit"] == MAX_LIMIT
    result = ask("high-value claims", gold_dir=GOLD, row_limit=2)
    assert len(result["rows"]) == 2
    assert len(result["rows"]) <= 2


def test_mirror_graph_matches_real_graph_on_supported_question():
    mirror = _MirrorGraph(GOLD)
    m = mirror.invoke(new_state("total claim amount by state"))
    r = ask("total claim amount by state", gold_dir=GOLD)
    assert m["engine"] == "mirror"
    assert m["rows"] == r["rows"]
    assert m["answer"] == r["answer"]
    assert m["sql"] == r["sql"]
