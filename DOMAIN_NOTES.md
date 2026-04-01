# DOMAIN_NOTES.md — Data Contract Enforcer

## Phase 0: Domain Reconnaissance

---

### Question 1: Backward-Compatible vs. Breaking Schema Changes

A **backward-compatible** change allows existing consumers to continue processing data without modification. A **breaking** change forces all downstream consumers to adapt or fail.

**Three backward-compatible examples from my systems:**

1. **Week 5 (Veritas Stream) — Adding `metadata.orchestrator_session_id` to event payloads.** This nullable field was added to track multi-agent orchestration sessions. Existing consumers that read `event_type` and `payload` ignore the new metadata key. No downstream breakage.

2. **Week 3 (PaperMind AI) — Adding `figures[]` array to extraction output.** The original extraction output contained `pages[]` and `tables[]`. Adding `figures[]` was additive — Week 4 Cartographer only consumed `doc_id` and `extracted_facts`, so the new array was invisible to it.

3. **Week 4 (Brownfield Cartographer) — Adding `embeddings.json` alongside `lineage_graph.json`.** A new output file was produced for semantic search. Existing consumers that only read `lineage_graph.json` were unaffected because the lineage graph schema itself was unchanged.

**Three breaking examples from my systems:**

1. **Week 3 (PaperMind AI) — Changing `confidence` from float 0.0–1.0 to integer 0–100.** Any consumer applying a threshold like `if confidence > 0.7` would silently pass every record when the values shift to 70–100. This is the canonical dangerous change because it passes type checks (still numeric) but breaks semantics.

2. **Week 5 (Veritas Stream) — Renaming `stream_id` to `aggregate_id`.** The canonical event_record schema uses `aggregate_id`, but the original Veritas output used `stream_id`. Any consumer doing `event["stream_id"]` would crash with a KeyError. This required a migration script during our data contract work.

3. **Week 2 (LangGraph Auditor) — Changing `overall_score` from float to categorical `overall_verdict` (PASS/FAIL/WARN).** The original system used a 1–5 numeric scale. The canonical verdict_record schema expects a string enum. Any consumer doing arithmetic on `overall_score` (e.g., averaging verdicts) would fail on the string type.

---

### Question 2: The Confidence Scale Change — Failure Trace

**Current state of my Week 3 confidence data:**

```
python3 -c "
import json, statistics
with open('outputs/week3/extractions.jsonl') as f:
    records = [json.loads(l) for l in f if l.strip()]
confs = [f['confidence'] for r in records for f in r.get('extracted_facts', [])]
print(f'count={len(confs)}')
print(f'min={min(confs):.4f} max={max(confs):.4f} mean={statistics.mean(confs):.4f}')
print(f'stdev={statistics.stdev(confs):.4f}')
"
```

**Output:**
```
count=28818
min=0.6718  max=1.0000  mean=0.8308
stdev=0.0477
```

All confidence values are in the 0.0–1.0 range. The mean of 0.8308 with stddev 0.0477 establishes the baseline.

**If an update changes confidence to 0–100 scale:**
- `min` jumps to ~67.18, `max` to 100.0, `mean` to ~83.08
- The Week 4 Cartographer consumes `extracted_facts` as node metadata. If it uses confidence to filter high-quality facts (`confidence >= 0.8`), it would now include everything since all values exceed 0.8 when expressed as 67–100. The lineage graph would be polluted with low-quality facts incorrectly treated as high-confidence.
- The statistical drift check would fire: z-score = |83.08 - 0.8308| / 0.0477 = ~1724, far exceeding the 3-stddev threshold.

**Bitol YAML contract clause that catches this:**

```yaml
extracted_facts_confidence:
  type: number
  minimum: 0.0
  maximum: 1.0
  required: true
  description: >
    Confidence score. Must remain 0.0-1.0 float.
    BREAKING if changed to 0-100 integer scale.
    Downstream consumers (Week 4 Cartographer) apply
    threshold filters assuming this range.
```

This range check (`max <= 1.0`) catches the violation immediately because the first value above 1.0 triggers a CRITICAL FAIL.

---

### Question 3: Blame Chain Construction Using the Lineage Graph

When the ValidationRunner detects a contract violation (e.g., `extracted_facts.confidence.range` FAIL), the ViolationAttributor traces the failure to its origin using the Week 4 lineage graph.

**Step-by-step graph traversal:**

1. **Parse the failing check_id.** From `week3.extracted_facts.confidence.range`, extract the system prefix (`week3`) and the column path (`extracted_facts.confidence`).

2. **Load the Week 4 lineage snapshot.** Read the latest record from `outputs/week4/lineage_snapshots.jsonl`. This contains `nodes[]` (files, tables, services) and `edges[]` (IMPORTS, CALLS, READS, WRITES, PRODUCES, CONSUMES relationships).

3. **Identify the origin node.** Search `nodes[]` for entries where `node_id` contains the system prefix (e.g., `file::src/week3/extractor.py`). These are candidate source files.

4. **Breadth-first upstream traversal.** Starting from the origin node, follow edges in reverse (target → source) where the relationship is PRODUCES or WRITES. Collect all upstream files that could have modified the confidence column. Stop at file-system root or external boundary.

5. **Git blame integration.** For each upstream file identified, run `git log --follow --since="14 days ago"` to find recent commits. Then run `git blame -L {line_range}` on files that contain confidence-related logic.

6. **Confidence scoring.** Each candidate commit receives a score: `base = 1.0 - (days_since_commit * 0.1)`. Reduce by 0.2 per lineage hop between the blamed file and the failing column. Rank candidates by score, return top 1–5.

7. **Blast radius computation.** From the contract's `lineage.downstream[]`, enumerate all nodes that consume the affected column. This gives the list of affected pipelines and estimated record count.

**Trust boundary caveat:** This lineage-traversal approach works because all five systems live within a single trust boundary — I built them all and have full graph visibility (**Tier 1: same team, same repo**). It would not work across organizational boundaries, where three tiers apply:

- **Tier 1 (same team/repo):** Full lineage graph available. ViolationAttributor traverses it. Complete blast radius computation. This is our project scope.
- **Tier 2 (different teams, same company):** Partial lineage graph. Teams cannot see inside each other's systems. A **contract registry** fills the gap — teams register dependencies centrally (DataHub, OpenMetadata, dbt Mesh), and the registry handles notification on breaking changes. Blast radius = registered consumers of the contract.
- **Tier 3 (different companies):** No lineage graph at all. Contract registry with a **subscription model**. Breaking changes are versioned (v1 → v2) with a deprecation window. Each company computes its own internal blast radius. The cross-company blast radius is just subscriber count.

The critical difference: in Tier 1, the **producer computes** blast radius by traversing consumers. In Tiers 2–3, the model **inverts** — **consumers register** their dependencies, and blast radius becomes a subscription lookup. Confluent Schema Registry takes this further by refusing to register breaking changes entirely (compatibility enforcement at write time), so the blast radius question never arises. Pact (contract testing for APIs) inverts it completely: consumers publish expectations as "pacts" that run in the producer's CI gate, blocking deployment if any consumer's pact fails.

---

### Question 4: LangSmith Trace Record Contract

```yaml
kind: DataContract
apiVersion: v3.0.0
id: langsmith-trace-records
info:
  title: LangSmith Trace Records
  version: 1.0.0
  owner: platform-team
  description: >
    Exported LangSmith traces from AI agent runs across Weeks 2-5.
    Each record represents one LLM call, chain execution, or tool invocation.
servers:
  local:
    type: local
    path: outputs/traces/runs.jsonl
    format: jsonl
schema:
  id:
    type: string
    format: uuid
    required: true
    unique: true
  name:
    type: string
    required: true
    description: Chain or LLM name.
  run_type:
    type: string
    required: true
    enum: [llm, chain, tool, retriever, embedding]
    description: Type of LangChain run.
  start_time:
    type: string
    format: date-time
    required: true
  end_time:
    type: string
    format: date-time
    required: true
  total_tokens:
    type: integer
    minimum: 0
    required: true
    description: Must equal prompt_tokens + completion_tokens.
  prompt_tokens:
    type: integer
    minimum: 0
    required: true
  completion_tokens:
    type: integer
    minimum: 0
    required: true
  total_cost:
    type: number
    minimum: 0.0
    required: true
    description: Cost in USD. Must be non-negative.
quality:
  type: SodaChecks
  specification:
    checks for traces:
      - missing_count(id) = 0
      - duplicate_count(id) = 0
      # Structural clause: end_time must be after start_time
      - "end_time > start_time"
      # Statistical clause: total_tokens must equal sum of sub-tokens
      - "total_tokens = prompt_tokens + completion_tokens"
      # AI-specific clause: cost per token should be stable
      - "avg(total_cost / greatest(total_tokens, 1)) < 0.01"
      - row_count >= 50
```

**Structural clause:** `run_type` enum — catches invalid run types from misconfigured LangChain integrations.

**Statistical clause:** `total_tokens = prompt_tokens + completion_tokens` — catches token accounting drift that indicates a broken integration or model version change.

**AI-specific clause:** Cost-per-token stability check — a sudden increase in `total_cost / total_tokens` signals a model upgrade (e.g., GPT-4 replacing GPT-3.5) that may affect budget projections. This is an AI-operations contract clause, not a standard data quality check.

---

### Question 5: Contract Staleness — The Biggest Failure Mode

**The most common failure mode of contract enforcement in production is contract staleness.** Contracts become stale when:

1. **Schema evolves but contracts are not updated.** A developer adds a new column, changes a type, or renames a field. The contract still describes the old schema. The ValidationRunner either reports false positives (flagging valid new data) or false negatives (missing violations in changed columns). Teams learn to ignore the alerts.

2. **Baselines are never re-established.** Statistical drift checks compare against a baseline from the first run. If the data distribution legitimately shifts (new market, new document types), the drift check fires constantly. Teams disable it instead of updating the baseline.

3. **No ownership.** Contracts have no designated owner. When a producer changes their output, nobody updates the contract because nobody is responsible for it.

**How our architecture prevents this:**

- **Auto-generation from live data.** The ContractGenerator re-profiles data on every run and writes a new schema snapshot. If the schema changes, the snapshot captures the change, and the SchemaEvolutionAnalyzer classifies it. This means contracts track reality instead of becoming stale assertions.

- **Timestamped snapshots.** Every generator run writes to `schema_snapshots/{contract_id}/{timestamp}.yaml`. The diff between consecutive snapshots detects drift automatically — no human needs to remember to update the contract.

- **Lineage-driven ownership (Tier 1).** Within our single trust boundary, the contract embeds `lineage.downstream[]` from the Week 4 Cartographer graph. When a contract violation is detected, the blast radius report names every affected consumer. This creates accountability — the downstream team sees their name in the violation report. However, this only works because we own and can traverse all five systems. In production (Tier 2–3), this would be replaced by a contract registry with consumer subscriptions — tools like Confluent Schema Registry enforce compatibility at write time so breaking changes are blocked before they ship, while dbt Mesh makes cross-team dependencies explicit through shared governance. The lineage graph approach is correct for this project's scope but would not generalize without a registry layer.

- **Baseline refresh on re-validation.** The ValidationRunner writes baselines on first run and the SchemaEvolutionAnalyzer can trigger a baseline refresh after a classified compatible change, preventing false drift alerts from legitimate shifts.
