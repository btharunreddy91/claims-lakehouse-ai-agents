# How to create agents (in this repo)

Coming Day 6. The pattern, in one paragraph:

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
