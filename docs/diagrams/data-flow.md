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

    W1 -->|"<b>outputs/week1/intent_records.jsonl</b><br/>{intent_id, description, code_refs[], governance_tags, created_at}<br/>Failure: No"| W2

    W3 -->|"<b>outputs/week3/extractions.jsonl</b><br/>{doc_id, extracted_facts[], entities[], extraction_model, extracted_at}<br/>Failure: <b>Yes</b> — confidence scale ambiguity (0.0-1.0 vs 0-100)"| W4

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

| # | From | To | Data Path | Failure History |
|---|------|----|-----------|-----------------|
| 1 | Week 1 Intent Correlator | Week 2 Digital Courtroom | `outputs/week1/intent_records.jsonl` | No |
| 2 | Week 3 Document Refinery | Week 4 Brownfield Cartographer | `outputs/week3/extractions.jsonl` | **Yes** — confidence scale 0.0-1.0 vs 0-100 ambiguity |
| 3 | Week 4 Brownfield Cartographer | Week 7 Data Contract Enforcer | `outputs/week4/lineage_snapshots.jsonl` | **Yes** — self-scan produced 0 nodes |
| 4 | Week 5 Event Sourcing Platform | Week 7 Data Contract Enforcer | `outputs/week5/events.jsonl` | **Yes** — `stream_id` → `aggregate_id` rename |
| 5 | Week 1 Intent Correlator | Week 7 Data Contract Enforcer | `outputs/week1/intent_records.jsonl` | No |
| 6 | Week 2 Digital Courtroom | Week 7 Data Contract Enforcer | `outputs/week2/verdicts.jsonl` | **Yes** — numeric score vs enum mismatch |
| 7 | Week 3 Document Refinery | Week 7 Data Contract Enforcer | `outputs/week3/extractions.jsonl` | **Yes** — confidence scale ambiguity |
| 8 | LangSmith | Week 7 Data Contract Enforcer | `outputs/traces/runs.jsonl` | No |
| 9 | Weeks 2,3,5 agents | LangSmith | (trace telemetry) | No |

**Red-highlighted systems** (Week 3, Week 5) have caused the most interface failures and are prioritized for contract enforcement.
