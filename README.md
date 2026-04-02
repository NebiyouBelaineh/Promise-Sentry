# Promise-Sentry

A schema integrity and lineage attribution system that turns inter-system data flows into machine-checked promises. When a promise is broken by a schema change, type drift, or statistical shift, Promise-Sentry catches it, traces it to the commit that caused it, and reports the blast radius.

## Prerequisites

- Python 3.12+
- PostgreSQL (for Week 5 event migration, port 5433)
- Ollama with `nomic-embed-text:v1.5` (for embedding drift detection)

```bash
git clone https://github.com/NebiyouBelaineh/Promise-Sentry.git
cd Promise-Sentry
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Environment

Copy the `.env` from your Week 5 veritas-stream project, or ensure `DATABASE_URL` is set to the apex_ledger PostgreSQL instance. The migration script reads this automatically.

Set `OLLAMA_HOST` if Ollama is not at the default `http://172.29.96.1:11434`.

## How to Run (5 Scripts End-to-End)

### 1. Generate contracts

```bash
# Week 3 extractions
python contracts/generator.py \
  --source outputs/week3/extractions.jsonl \
  --contract-id week3-document-refinery-extractions \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --output generated_contracts/

# Week 5 events
python contracts/generator.py \
  --source outputs/week5/events.jsonl \
  --contract-id week5-event-records \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --output generated_contracts/
```

**Expected:** `generated_contracts/week3_extractions.yaml` (14 clauses), `week5_events.yaml` (141 clauses), plus dbt schema.yml files and schema snapshots.

### 2. Run validation

```bash
# Week 3 (clean data)
python contracts/runner.py \
  --contract generated_contracts/week3_extractions.yaml \
  --data outputs/week3/extractions.jsonl \
  --output validation_reports/week3_baseline.json

# Week 5 (real data quality findings)
python contracts/runner.py \
  --contract generated_contracts/week5_events.yaml \
  --data outputs/week5/events.jsonl \
  --output validation_reports/week5_db.json
```

**Expected:** Week 3: 47 checks, all PASS. Week 5: 299 checks (270 PASS, 29 FAIL on real findings: non-UUID payload IDs, boolean enum mismatches, hash pattern violations).

### 3. Attribute violations

```bash
python contracts/attributor.py \
  --report validation_reports/week5_db.json \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --repo-map week5=~/tenx/week5/veritas-stream \
  --output violation_log/violations.jsonl
```

**Expected:** 29 violations written with blame chains (git commit, author) and blast radius from lineage graph.

### 4. Analyze schema evolution

```bash
python contracts/schema_analyzer.py \
  --snapshots schema_snapshots/week5-event-records/ \
  --output schema_snapshots/week5-event-records/evolution_report.json
```

**Expected:** Diffs the two most recent snapshots. Reports 6 changes (5 backward-compatible, 1 breaking), classification: BREAKING, with rollback plan.

### 5. Run AI extensions

```bash
python contracts/ai_extensions.py \
  --week3 outputs/week3/extractions.jsonl \
  --week2 outputs/week2/verdicts.jsonl \
  --output validation_reports/ai_extensions.json
```

**Expected:** Three checks: embedding drift (PASS/BASELINE_SET), prompt input schema (PASS), output violation rate (PASS). First run sets embedding baseline; second run measures drift.

Use `--skip-embeddings` if Ollama is not available.

### 6. Generate enforcer report

```bash
python contracts/report_generator.py \
  --validation-reports validation_reports/ \
  --violations violation_log/violations.jsonl \
  --evolution schema_snapshots/week5-event-records/evolution_report.json \
  --ai-extensions validation_reports/ai_extensions.json \
  --output enforcer_report/report_data.json
```

**Expected:** `enforcer_report/report_data.json` with data health score (0-100), top 3 violations in plain language, schema evolution summary, AI extension results, and recommendations.

## Run Tests

```bash
pytest tests/ -v
```

All tests run without external dependencies (Ollama calls are mocked).

## Repository Structure

```
Promise-Sentry/
├── contracts/
│   ├── generator.py           # ContractGenerator: profiles data, produces Bitol YAML + dbt
│   ├── runner.py              # ValidationRunner: structural + statistical + cross-column checks
│   ├── attributor.py          # ViolationAttributor: git blame + lineage traversal + blast radius
│   ├── schema_analyzer.py     # SchemaEvolutionAnalyzer: diff, classify, rollback plan
│   ├── ai_extensions.py       # AI extensions: embedding drift, prompt schema, output violations
│   └── report_generator.py    # ReportGenerator: health score + plain-language enforcer report
├── generated_contracts/       # Auto-generated Bitol YAML + dbt schema.yml
├── validation_reports/        # Structured validation report JSON
├── violation_log/             # Attributed violation records (JSONL)
├── enforcer_report/           # Auto-generated enforcer report
├── schema_snapshots/          # Timestamped schema snapshots + baselines
├── outputs/                   # Canonical JSONL data from Weeks 1-5
│   ├── migrate/               # Migration scripts from prior-week formats
│   ├── week1/                 # intent_records.jsonl (6 records)
│   ├── week2/                 # verdicts.jsonl (6 records)
│   ├── week3/                 # extractions.jsonl (1,096 records, 28K+ facts)
│   ├── week4/                 # lineage_snapshots.jsonl (5 snapshots, 2,215 nodes)
│   └── week5/                 # events.jsonl (2,542 records from PostgreSQL)
├── tests/                     # pytest test suite (111 tests)
├── DOMAIN_NOTES.md            # Phase 0 domain reconnaissance
└── docs/                      # Reports, diagrams, guides
```

## Data Sources

| Week | System | Records | Key Schema |
|------|--------|---------|------------|
| 1 | Roo-Code (Intent Correlator) | 6 | intent_id, code_refs[], governance_tags |
| 2 | LangGraph Auditor (Digital Courtroom) | 6 | verdict_id, scores, overall_verdict, confidence |
| 3 | PaperMind AI (Document Refinery) | 1,096 | doc_id, extracted_facts[].confidence, entities[] |
| 4 | Brownfield Cartographer | 5 snapshots | snapshot_id, nodes[], edges[] |
| 5 | Veritas Stream (Event Sourcing) | 2,542 | event_id, event_type, payload, metadata |
