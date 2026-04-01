# Promise-Sentry

A schema integrity and lineage attribution system that turns inter-system data flows into machine-checked promises. When a promise is broken — by a schema change, type drift, or statistical shift — Promise-Sentry catches it, traces it to the commit that caused it, and reports the blast radius.

## Prerequisites

```bash
git clone https://github.com/NebiyouBelaineh/Promise-Sentry.git
cd Promise-Sentry
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## How to Run

### Step 1: Generate contracts

```bash
python contracts/generator.py \
  --source outputs/week3/extractions.jsonl \
  --contract-id week3-document-refinery-extractions \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --output generated_contracts/
```

**Expected output:** `generated_contracts/week3_extractions.yaml` (14 schema clauses + 2 cross-column constraints) and `generated_contracts/week3_extractions_dbt.yml`

```bash
python contracts/generator.py \
  --source outputs/week5/events.jsonl \
  --contract-id week5-event-records \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --output generated_contracts/
```

**Expected output:** `generated_contracts/week5_events.yaml` (137 schema clauses + 3 cross-column constraints) and `generated_contracts/week5_events_dbt.yml`

### Step 2: Run validation (clean data)

```bash
python contracts/runner.py \
  --contract generated_contracts/week3_extractions.yaml \
  --data outputs/week3/extractions.jsonl \
  --output validation_reports/week3_clean.json
```

**Expected output:** `validation_reports/week3_clean.json` — 47 checks, all PASS

```bash
python contracts/runner.py \
  --contract generated_contracts/week5_events.yaml \
  --data outputs/week5/events.jsonl \
  --output validation_reports/week5_clean.json
```

**Expected output:** `validation_reports/week5_clean.json` — 292 checks (265 PASS, 27 FAIL on real data quality findings: non-UUID payload IDs, boolean enum mismatches across event types)

### Step 3: Verify output

After running all steps, open any validation report and verify `total_checks > 0` and the JSON structure contains `report_id`, `contract_id`, `results[]` with per-check status.

## Repository Structure

```
Promise-Sentry/
├── contracts/
│   ├── generator.py           # ContractGenerator — profiles data, produces Bitol YAML + dbt
│   └── runner.py              # ValidationRunner — structural + statistical + cross-column checks
├── generated_contracts/       # Auto-generated contract YAML + dbt schema.yml
├── validation_reports/        # Structured validation report JSON
├── schema_snapshots/          # Timestamped schema snapshots + baselines
├── outputs/                   # Canonical JSONL data from Weeks 1–5
│   ├── migrate/               # Migration scripts from prior-week formats
│   ├── week1/                 # intent_records.jsonl (6 records)
│   ├── week2/                 # verdicts.jsonl (6 records)
│   ├── week3/                 # extractions.jsonl (1,096 records)
│   ├── week4/                 # lineage_snapshots.jsonl (5 snapshots)
│   └── week5/                 # events.jsonl (1,198 records)
├── DOMAIN_NOTES.md            # Phase 0 domain reconnaissance (5 questions)
└── docs/
    ├── diagrams/              # Mermaid data flow diagram
    └── guide/                 # Interim guide and report draft
```

## Data Sources

| Week | System | Repository | Records | Key Schema |
|------|--------|------------|---------|------------|
| 1 | Roo-Code (Intent Correlator) | [Roo-Code](https://github.com/NebiyouBelaineh/Roo-Code) | 6 | intent_id, code_refs[], governance_tags |
| 2 | LangGraph Auditor (Digital Courtroom) | [LangGraph-Automation-Auditor](https://github.com/NebiyouBelaineh/LangGraph-Automation-Auditor) | 6 | verdict_id, scores, overall_verdict |
| 3 | PaperMind AI (Document Refinery) | [paperMind-ai](https://github.com/NebiyouBelaineh/paperMind-ai) | 1,096 | doc_id, extracted_facts[], entities[] |
| 4 | Brownfield Cartographer | [brownfield-cartographer](https://github.com/NebiyouBelaineh/brownfield-cartographer) | 5 | snapshot_id, nodes[], edges[] |
| 5 | Veritas Stream (Event Sourcing) | [veritas-stream](https://github.com/NebiyouBelaineh/veritas-stream) | 1,198 | event_id, event_type, payload, metadata |
