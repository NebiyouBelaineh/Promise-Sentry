# Data Contract Enforcer — Interim Report (Thursday)

**Author:** Nebiyou Belaineh
**Date:** April 2026
**Repository:** [Promise-Sentry GitHub Link]

---

## 1. Data Flow Diagram

# Inter-System Data Flow Diagram

```mermaid
graph TD
    W1["<b>Week 1</b><br/>Intent-Code Correlator<br/><i>Roo-Code</i>"]
    W2["<b>Week 2</b><br/>Digital Courtroom<br/><i>LangGraph Automation Auditor</i>"]
    W3["<b>Week 3</b><br/>Document Refinery<br/><i>PaperMind AI</i>"]
    W4["<b>Week 4</b><br/>Brownfield Cartographer<br/><i>brownfield-cartographer</i>"]
    W5["<b>Week 5</b><br/>Event Sourcing Platform<br/><i>Veritas Stream</i>"]
    LS["<b>LangSmith</b><br/><i>(External)</i>"]
    W7["<b>Week 7</b><br/>Data Contract Enforcer<br/><i>Promise-Sentry</i>"]

    W1 -.->|"<b>⚠ THEORETICAL</b><br/>outputs/week1/intent_records.jsonl<br/>{intent_id, description, code_refs[], governance_tags, created_at}<br/>Failure: No"| W2

    W3 -.->|"<b>⚠ THEORETICAL</b><br/>outputs/week3/extractions.jsonl<br/>{doc_id, extracted_facts[], entities[], extraction_model, extracted_at}<br/>Failure: No — not implemented in practice"| W4

    W4 -->|"<b>outputs/week4/lineage_snapshots.jsonl</b><br/>{snapshot_id, codebase_root, git_commit, nodes[], edges[], captured_at}<br/>Failure: <b>Yes</b> — brownfield-cartographer self-scan produced 0 nodes"| W7

    W5 -->|"<b>outputs/week5/events.jsonl</b><br/>{event_id, event_type, aggregate_id, sequence_number, payload, metadata, occurred_at, recorded_at}<br/>Failure: <b>Yes</b> — stream_id→aggregate_id rename broke consumers"| W7

    W1 -->|"<b>outputs/week1/intent_records.jsonl</b><br/>{intent_id, code_refs[].file}<br/>Failure: No"| W7

    W2 -->|"<b>outputs/week2/verdicts.jsonl</b><br/>{verdict_id, scores, overall_verdict, confidence, evaluated_at}<br/>Failure: <b>Yes</b> — numeric score vs PASS/FAIL/WARN enum mismatch"| W7

    W3 -->|"<b>outputs/week3/extractions.jsonl</b><br/>{doc_id, extracted_facts[].confidence, entities[]}<br/>Failure: <b>Yes</b> — confidence scale ambiguity"| W7

    LS -->|"<b>outputs/traces/runs.jsonl</b><br/>{id, name, run_type, total_tokens, prompt_tokens, completion_tokens, total_cost, start_time, end_time}<br/>Failure: No"| W7

    W3 -.->|"Agents emit traces"| LS
    W5 -.->|"Agents emit traces"| LS
    W2 -.->|"Agents emit traces"| LS

    style W7 fill:#2d6a4f,stroke:#1b4332,color:#fff
    style LS fill:#495057,stroke:#343a40,color:#fff
    style W1 fill:#264653,stroke:#1d3557,color:#fff
    style W2 fill:#264653,stroke:#1d3557,color:#fff
    style W3 fill:#e76f51,stroke:#c1440e,color:#fff
    style W4 fill:#264653,stroke:#1d3557,color:#fff
    style W5 fill:#e76f51,stroke:#c1440e,color:#fff
```

## Summary of Interfaces

| # | From | To | Data Path | Type | Failure History |
|---|------|----|-----------|------|-----------------|
| 1 | Week 1 Intent Correlator | Week 2 Digital Courtroom | `outputs/week1/intent_records.jsonl` | Theoretical | No |
| 2 | Week 3 Document Refinery | Week 4 Brownfield Cartographer | `outputs/week3/extractions.jsonl` | Theoretical | No |
| 3 | Week 4 Brownfield Cartographer | Week 7 Data Contract Enforcer | `outputs/week4/lineage_snapshots.jsonl` | **Real** | **Yes** — self-scan produced 0 nodes |
| 4 | Week 5 Event Sourcing Platform | Week 7 Data Contract Enforcer | `outputs/week5/events.jsonl` | **Real** | **Yes** — `stream_id` → `aggregate_id` rename |
| 5 | Week 1 Intent Correlator | Week 7 Data Contract Enforcer | `outputs/week1/intent_records.jsonl` | **Real** | No |
| 6 | Week 2 Digital Courtroom | Week 7 Data Contract Enforcer | `outputs/week2/verdicts.jsonl` | **Real** | **Yes** — numeric score vs enum mismatch |
| 7 | Week 3 Document Refinery | Week 7 Data Contract Enforcer | `outputs/week3/extractions.jsonl` | **Real** | **Yes** — confidence scale ambiguity |
| 8 | LangSmith | Week 7 Data Contract Enforcer | `outputs/traces/runs.jsonl` | **Real** | No |
| 9 | Weeks 2,3,5 agents | LangSmith | (trace telemetry) | **Real** | No |

**Red-highlighted systems** (Week 3, Week 5) have caused the most interface failures and are prioritized for contract enforcement.

> **Note on theoretical vs. real links:** The canonical challenge spec defines inter-system dependencies (e.g., Week 1 → Week 2, Week 3 → Week 4) as part of the target architecture. In practice, these links were not implemented — the Auditor (Week 2) does not consume Roo-Code intent records, and the Cartographer (Week 4) scans external codebases rather than PaperMind extraction output. These arrows are shown as dashed lines labelled "THEORETICAL" to distinguish them from the real data flows where Week 7 consumes all prior-week outputs directly. The contracts enforce the schemas as they exist today; the theoretical links indicate where future integration could formalize currently-absent dependencies.


```
┌──────────────────┐     intent_record        ┌──────────────────┐
│  Week 1          │ ──────────────────────►   │  Week 2          │
│  Roo-Code        │   code_refs[].file =      │  LangGraph       │
│  Intent-Code     │   verdict.target_ref      │  Automation      │
│  Correlator      │                           │  Auditor         │
└──────────────────┘                           └──────────────────┘
                                                       │
                                                       │ verdict_record
                                                       ▼
┌──────────────────┐     extraction_record     ┌──────────────────┐
│  Week 3          │ ──────────────────────►   │  Week 4          │
│  PaperMind AI    │   doc_id → node           │  Brownfield      │
│  Document        │   facts → metadata        │  Cartographer    │
│  Refinery        │                           │                  │
└──────────────────┘                           └──────────────────┘
        │                                              │
        │ extraction_record                            │ lineage_snapshot
        ▼                                              ▼
┌──────────────────┐                           ┌──────────────────┐
│  Week 5          │                           │  Week 7          │
│  Veritas Stream  │  event_record             │  Promise-Sentry  │
│  Event Sourcing  │ ──────────────────────►   │  Data Contract   │
│  Platform        │                           │  Enforcer        │
└──────────────────┘                           └──────────────────┘
                                                       ▲
┌──────────────────┐     trace_record                  │
│  LangSmith       │ ──────────────────────────────────┘
│  (External)      │
└──────────────────┘
```

---

## 2. Contract Coverage Table

| # | Interface (Arrow) | Producer | Consumer | Contract Written? | Notes |
|---|-------------------|----------|----------|-------------------|-------|
| 1 | `intent_record.code_refs[]` → `verdict.target_ref` | Week 1 Roo-Code | Week 2 Auditor | No | Week 1 has only 6 records; low priority for contract enforcement |
| 2 | `extraction_record` → lineage node metadata | Week 3 PaperMind | Week 4 Cartographer | **Yes** | `week3_extractions.yaml` — 14 clauses including confidence range |
| 3 | `lineage_snapshot` → ViolationAttributor | Week 4 Cartographer | Week 7 Enforcer | Partial | Lineage data used for blame chain; contract deferred to final submission |
| 4 | `event_record` → schema contract | Week 5 Veritas Stream | Week 7 Enforcer | **Yes** | `week5_events.yaml` — 137 clauses covering all payload fields |
| 5 | `trace_record` → AI Contract Extension | LangSmith | Week 7 Enforcer | No | Deferred to final submission (AI Extensions phase) |
| 6 | `verdict_record` → LLM output validation | Week 2 Auditor | Week 7 Enforcer | No | Deferred to final submission (AI Extensions phase) |

**Coverage:** 2/6 interfaces have full contracts (33%). The two covered interfaces (Week 3 extractions, Week 5 events) are the highest-data-volume and most critical for downstream consumption.

---

## 3. First Validation Run Results

### Week 3 — Document Refinery Extractions

```
Data:     outputs/week3/extractions.jsonl (1,096 records → 28,818 rows after flattening)
Contract: generated_contracts/week3_extractions.yaml (14 clauses)

Total checks: 44
  PASS:  44
  FAIL:   0
  WARN:   0
  ERROR:  0
```

All 44 checks passed on clean data. Key checks include:
- `extracted_facts_confidence.range`: min=0.6718, max=1.0000 — within 0.0–1.0 contract
- `doc_id.required`: no nulls across 28,818 rows
- `doc_id.uuid`: all values match UUID pattern
- `extracted_at.datetime`: all values parse as ISO 8601

### Week 5 — Event Sourcing Platform

```
Data:     outputs/week5/events.jsonl (1,198 records)
Contract: generated_contracts/week5_events.yaml (137 clauses)

Total checks: 266
  PASS:  243
  FAIL:   23
  WARN:    0
  ERROR:   0
```

23 checks failed — all are **real data quality findings**, not bugs in the runner:
- **11 UUID format violations**: Payload fields like `application_id`, `package_id`, `session_id` contain application-specific IDs (e.g., "APEX-0001") rather than UUID v4 format. The contract correctly identifies these as non-conforming to the `_id` → UUID convention.
- **12 enum violations**: Boolean payload fields (`is_coherent`, `has_quality_flags`, `has_hard_block`) have mixed null/non-null populations across event types. Records for event types that don't use these fields have nulls, which the enum check flags.

These findings demonstrate the ValidationRunner correctly detecting structural inconsistencies in real data.

---

## 4. Reflection

The most surprising discovery was a naming convention collision in the Week 5 Veritas Stream events. Payload fields ending with `_id` — like `application_id`, `session_id`, and `package_id` — contain application-specific identifiers such as "APEX-0001" and "docpkg-APEX-0001", not UUID v4 strings. When the ContractGenerator profiled these columns, it correctly inferred the `_id` suffix convention and generated a UUID format clause. The ValidationRunner then flagged 11 UUID format violations across 1,198 records. This is not a bug — it is a genuine ambiguity between system identifiers (which should be UUIDs for global uniqueness) and domain identifiers (which carry business meaning in their structure). Without a contract making this distinction explicit, a downstream consumer parsing `application_id` as a UUID would silently fail. The contract now documents which `_id` fields are UUIDs and which are domain-structured strings.

The Week 3 PaperMind confidence distribution revealed something I had not considered. All 28,818 extracted fact confidence scores fall between 0.67 and 1.00, with a mean of 0.83 and standard deviation of only 0.048. There are no low-confidence extractions at all. This means either the extraction model is uniformly confident about everything it produces — which is suspicious and suggests poor calibration — or low-confidence extractions were filtered out upstream in the PaperMind pipeline before reaching the output JSONL. Either way, the narrow distribution is now captured as a baseline. If a future model change or pipeline modification shifts the mean outside this band, the statistical drift check will catch it. Before writing the contract, this distribution was invisible — the data existed but nobody had profiled it.

The Week 4 Cartographer exposed an ironic blind spot: it maps the dependency graphs of five external codebases (dbt-core, airflow, sqlalchemy, ol-data-platform, jaffle-shop) but produces zero nodes when scanning its own repository. The `brownfield-cartographer` entry in `.cartography/` has an empty lineage graph. This means the tool that generates the lineage data used by the ViolationAttributor cannot trace violations within itself. The contract coverage table made this gap visible immediately — without it, the assumption that "Week 4 covers all systems" would have gone unquestioned. This is the kind of discovery that validates the entire exercise: the contract enforcer's first finding was about the contract enforcer's own dependency.

---

*End of interim report.*
