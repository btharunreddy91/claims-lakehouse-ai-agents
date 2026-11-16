# How to create agents (in this repo)

An agent = **LangGraph state machine** with a typed state, nodes that do one
job, tools the nodes can call, and a router that decides the next node.
No node ever writes to production directly. Risky steps pause for a human.

```
User question
  → Supervisor: classify (analytics / pipeline / quality)
  → Agent: plan → call tools → check guardrails → draft answer / code
  → Human-in-the-loop (if write / deploy / PII)
  → Answer, with the SQL / code / evidence shown
```

Each agent gets its own file under `src/agents/`, its own tools, its own
eval set under `tests/`, and a guardrail checklist in its docstring.

## Day 6 — Analytics Agent v1 (`src/agents/analytics.py`)

Text-to-SQL over the **Gold marts only** (`fct_claims`, `dim_customer`,
`dim_policy`, `agg_claims_daily_state` from Day 5). The agent never
touches Bronze/Silver directly and never writes anything.

### State

`AgentState` (TypedDict): `question`, `intent`, `sql`, `rows`, `answer`,
`errors`, `row_limit`, `engine`. One state dict flows through every
node; nodes return partial updates, never side effects on each other.

### Nodes

| Node | Job |
|---|---|
| `plan` | Rule-based planner (`plan_question`): keyword/intent matching over the supported questions below. Returns `(intent, sql)` or `(None, None)` — it never guesses SQL. |
| `validate` | `validate_sql`: the read-only guardrail (see below). |
| `execute` | `execute_sql`: runs the validated SELECT against Gold loaded into in-memory SQLite (stdlib). Rows are PII-masked on the way out. |
| `answer` | Formats the human answer from the rows. List answers say how many rows are *shown* and flag when the row limit capped the result — a capped slice is never presented as the whole population. |

Routing: `plan → validate → execute → answer`, with conditional edges —
after `plan`, if there is no SQL or an error, go straight to `answer`;
same after `validate`. The graph is built by `build_graph()` and run
by `ask(question, gold_dir, row_limit)`.

### Tools

- `validate_sql(sql)` — the guardrail (below).
- `execute_sql(sql, gold_dir, limit)` — SQLite execution over Gold.
- `mask_row` / `mask_value` — PII masking.
- `plan_with_llm(question, llm)` — the LLM seam (below).

### Guardrail checklist

- **Gold only.** Any table outside `fct_claims`, `dim_customer`,
  `dim_policy`, `agg_claims_daily_state` is rejected.
- **Read-only.** SQL must be a single SELECT. Rejected: INSERT / UPDATE /
  DELETE / DROP / ALTER / CREATE / TRUNCATE / REPLACE / MERGE / GRANT /
  REVOKE / PRAGMA / ATTACH / VACUUM / COPY / INTO, multiple statements,
  `UNION` / `EXCEPT` / `INTERSECT`, subqueries, and functions outside
  `COUNT, SUM, AVG, MIN, MAX, ROUND, COALESCE`. Comments are inspected
  *before* stripping: a comment containing `;` or a write keyword is
  itself a rejection (no smuggling a second statement in `--` or `/* */`).
- **Row limit.** Default 100, hard max 1000 (`apply_limit` clamps any
  LIMIT the SQL already carries).
- **PII.** Customer names are reduced to initials (`Lena Patel` → `L. P.`);
  `policy_id` / `customer_id` are reduced to their last 4 characters
  (`POL-80239` → `****0239`). `claim_id` is a synthetic claim reference,
  not a person identifier, and is kept so a claim can be referenced.
- **SQL is always shown** to the user, in the CLI and in the state.

### The LLM seam (no LLM in v1)

`plan_with_llm(question, llm=None)` is where a real LLM replaces the
rule planner later (Day 9+). Pass a callable that takes a prompt and
returns a SQL string; the returned SQL still goes through
`validate_sql` before it can execute. With no callable it raises
`NotImplementedError` — v1 never invents an API key or a network call.
`ask(..., llm=callable)` routes the plan step through it.

### How to run

```bash
make ask Q="total claim amount by state"
# .venv/bin/python -m src.agents.analytics "total claim amount by state"
# .venv/bin/python -m src.agents.cli "high-value claims" --limit 3
# .venv/bin/python -m src.agents.analytics "claims between 2026-01-01 and 2026-03-31"
```

If `data/gold/` is missing, the agent rebuilds it from enriched Silver
via Day 5's `build_gold` (`--rebuild-gold` forces it). Unknown questions
exit non-zero with a "not supported yet" answer and no SQL.

### What is real vs not (Nov 15)

- **Real LangGraph:** `langgraph==1.2.22` installed in the repo `.venv`
  and verified (`StateGraph`, `END`, compiled graph). The CLI reports
  `Engine: langgraph` there.
- **Stdlib mirror:** the same node functions behind `_MirrorGraph`,
  used automatically when LangGraph is not importable — e.g. system
  `python3`, where `pip install langgraph` fails (PEP 668 externally
  managed environment; the Debian `typing_extensions 4.10.0` cannot be
  uninstalled). Verified: `python3 -m src.agents.analytics ...` reports
  `Engine: mirror` and returns identical rows/answers. Tests cover
  both engines.
- **No LLM yet:** the planner is deterministic rules, not a model.
  It answers only the supported questions below and says so otherwise.

### Supported questions

Total claims / total amount; by state; by claim type; by status;
top N states / top N customers by amount; high-value claims
(over $25,000); claims in a state; claims between two dates;
average claim amount; claims per day.

### Example Q&As (real dry-run numbers, Nov 15, over the real Gold)

- *"total claim amount"* → 758 claims totalling **$11,975,925.09**
  (reconciles with the Day 5 Gold manifest).
- *"total claim amount by state"* → 10 states. Top: TX 184 claims,
  **$2,743,287.02**; CA 129, $1,970,269.02; NY 88, $1,529,507.46;
  FL 92, $1,177,052.32; AZ 66, $1,088,297.48.
- *"total claim amount by claim type"* → property 202 claims,
  $4,686,581.58; life 85, $4,324,320.32; auto 272, $2,295,269.71;
  health 199, $669,753.48.
- *"average claim amount"* → **$15,799.37** across 758 claims.
- *"high-value claims" --limit 3* → top: CLM-0000008, AZ, life,
  **$126,633.20** (2026-02-21); next $108,699.45 and $104,823.54.
  The 3 shown total $340,156.19; all 166 high-value claims are in Gold.
- *"top 3 customers by total claim amount"* → top customer (masked)
  A. S., 2 claims, $132,482.07.
- *"claims between 2026-01-01 and 2026-03-31"* → 100 rows shown
  (default limit), $1,281,438.52 over the rows shown — the answer
  says the limit was reached rather than claiming 100 is the Q1 total.
- *"will it rain tomorrow"* → no SQL, "Question not supported yet",
  exit code 1.

### Tests

`tests/test_analytics_agent.py` — 50 tests: validator rejections
(write keywords, multi-statement, comment smuggling, UNION,
unknown tables, subqueries), planner coverage for every supported
intent, reconciliation over real Gold (total $11,975,925.09, 758
claims, 166 high-value, by-state sums = total), masking, unknown
questions, row-limit clamping, and mirror-vs-LangGraph equivalence.
Full suite: **87 passed** (37 existing + 50 new).
